use epoll;

pub fn read_event(fd: i32) -> epoll::Event {
    epoll::Event {
        data: fd as u64,
        events: epoll::Events::EPOLLIN.bits()
            | epoll::Events::EPOLLERR.bits()
            | epoll::Events::EPOLLHUP.bits(),
    }
}
