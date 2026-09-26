use std::io;
use std::thread;

use tokio::sync::mpsc;

use super::Request;
use super::audit::InputAudit;
use super::audit::InputCompletion;

#[derive(Debug)]
pub(super) struct InputStopped;

impl std::fmt::Display for InputStopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("stdin stop requested")
    }
}
impl std::error::Error for InputStopped {}

pub(super) struct InputWorker {
    #[cfg(unix)]
    stop: std::os::unix::net::UnixStream,
    task: thread::JoinHandle<io::Result<InputCompletion>>,
}

pub(super) struct InputStartFailure {
    pub(super) error: io::Error,
    pub(super) completion: io::Result<InputCompletion>,
}

impl InputStartFailure {
    fn new(error: io::Error, audit: InputAudit) -> Self {
        Self {
            error,
            completion: audit.close_unstarted(),
        }
    }
}

impl InputWorker {
    pub(super) fn start(
        audit: InputAudit,
        sender: mpsc::Sender<io::Result<Request>>,
    ) -> Result<Self, InputStartFailure> {
        // Retain journals in the caller until the OS has created the worker.
        // A failed spawn drops its closure, so moving audit into it would lose
        // the only owner able to record the producer's final positions.
        let (handoff, receive) = std::sync::mpsc::sync_channel(1);
        let started = (|| -> io::Result<Self> {
            #[cfg(unix)]
            let (stop, input) = {
                use std::os::fd::FromRawFd;
                let (stop, wake) = std::os::unix::net::UnixStream::pair()?;
                let minimum_fd: libc::c_int = 0;
                // SAFETY: fcntl borrows stdin and duplicates it with close-on-exec,
                // returning an owned descriptor or -1. The command takes one int.
                let fd =
                    unsafe { libc::fcntl(libc::STDIN_FILENO, libc::F_DUPFD_CLOEXEC, minimum_fd) };
                if fd < 0 {
                    return Err(io::Error::last_os_error());
                }
                // SAFETY: fd is the newly duplicated descriptor, owned exactly once.
                let input = unsafe { std::fs::File::from_raw_fd(fd) };
                (stop, InterruptibleInput { input, wake })
            };
            let task = thread::Builder::new()
                .name("machine-stdin".to_owned())
                .spawn(move || {
                    let (audit, sender): (InputAudit, _) =
                        receive.recv().map_err(io::Error::other)?;
                    #[cfg(not(unix))]
                    let input = io::stdin().lock();
                    audit.read_requests(sender, input)
                })?;
            Ok(Self {
                #[cfg(unix)]
                stop,
                task,
            })
        })();
        match started {
            Ok(worker) => match handoff.send((audit, sender)) {
                Ok(()) => Ok(worker),
                Err(error) => Err(InputStartFailure::new(
                    io::Error::other("stdin worker ended before journal handoff"),
                    error.0.0,
                )),
            },
            Err(error) => Err(InputStartFailure::new(error, audit)),
        }
    }

    /// Unix readers are woken independently of stdin activity. On other hosts,
    /// the control input producer must close stdin to finish the reader.
    pub(super) async fn stop(self) -> io::Result<InputCompletion> {
        #[cfg(unix)]
        drop(self.stop);
        tokio::task::spawn_blocking(move || {
            self.task
                .join()
                .map_err(|_| io::Error::other("stdin recorder panicked"))?
        })
        .await
        .map_err(io::Error::other)?
    }
}

#[cfg(unix)]
struct InterruptibleInput {
    input: std::fs::File,
    wake: std::os::unix::net::UnixStream,
}

#[cfg(unix)]
impl io::Read for InterruptibleInput {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        use std::os::fd::AsRawFd;
        if bytes.is_empty() {
            return Ok(0);
        }
        loop {
            let mut fds = [
                libc::pollfd {
                    fd: self.input.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                },
                libc::pollfd {
                    fd: self.wake.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                },
            ];
            // SAFETY: both descriptors remain owned by self for this call and
            // the mutable array contains exactly nfds initialized poll entries.
            let ready = unsafe {
                libc::poll(
                    fds.as_mut_ptr(),
                    fds.len() as libc::nfds_t,
                    /*timeout*/ -1,
                )
            };
            if ready < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            if fds[1].revents != 0 {
                return Err(io::Error::other(InputStopped));
            }
            if fds[0].revents != 0 {
                return self.input.read(bytes);
            }
        }
    }
}
