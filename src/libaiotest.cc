#include <iostream>
#include <iomanip>
#include <fcntl.h>
#include <libaio.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <fcntl.h>
#include <errno.h>
#include <libaio.h>

#include <stdio.h>
#include <stddef.h>
#include <ctype.h>

int const nprint = 16;

io_context_t ctx;
struct iocb iocb0;

#define showField(str, field) \
  << (",\n " #field  "=") << ev.field << "\t  [" << sizeof(str.field) << "@" << offsetof(typeof(str), field) << "]"

std::ostream& operator<< (std::ostream& out, iocb const &ev) {
  out << "IOCB(@" << (void*) &ev << ", size " << sizeof(ev)
    showField(ev, data)
    showField(ev, key)
    showField(ev, aio_rw_flags)
    showField(ev, aio_lio_opcode)
    showField(ev, aio_reqprio)
    showField(ev, aio_fildes)
    showField(ev, u.c.buf)
    showField(ev, u.c.nbytes)
    showField(ev, u.c.offset)
      << ")\n";
  if (ev.u.c.buf) {
    out << "buf = '" << std::hex;
    for (int i = 0; i < std::min<int>(12, ev.u.c.nbytes); ++i) {
      char c = ((char const*)ev.u.c.buf)[i];
      if (isascii(c)) {
        out << c;
      } else {
        out << "\\" << std::setw(2) << std::setfill('0') << int(c);
      }
    }
    out << std::dec;
    out << "\n";
  }
  return out;
}

std::ostream& operator<< (std::ostream& out, io_event const &ev) {
  out << "IOEvent(@" << (void*) &ev << ", size " << sizeof(ev)
    showField(ev, data)
    showField(ev, obj)
    showField(ev, res)
    showField(ev, res2)
      << ")\n";
  if (ev.obj) {
    iocb* cb = (iocb*) ev.obj;
    out << "CB: " << *cb << "\n";
  }
  return out;
}


extern "C" int my_timespec_info(struct timespec* ptr) {
  printf("TIMESPEC at %#x size %#x\n", ptr, sizeof(struct timespec));
  printf("TIMESPEC->tv_sec = %#x size %#x at offs %#x\n",
         ptr->tv_sec, sizeof(ptr->tv_sec), offsetof(struct timespec, tv_sec));
  printf("TIMESPEC->tv_nsec = %#x size %#x at offs %#x\n",
         ptr->tv_nsec, sizeof(ptr->tv_nsec), offsetof(struct timespec, tv_nsec));
  return 0;
}

extern "C" int my_io_info(io_context_t* ctx, struct iocb* ptr, sigset_t* sigset,
                          struct io_event* event, struct io_event* events, struct timespec *timespec) {
  if (ptr)
    std::cout << "cb: " << *ptr << "\n";
  if (events)
    std::cout << "events[0]: " << *events << "\n";

  if (timespec) {
    my_timespec_info(timespec);
  }

  return 0;
}


extern "C" int my_io_event_info(io_event* event) {
  std::cout << "event @" << (void*)event << "\n";
  if (event)
    std::cout << "event: " << *event << "\n";

  std::cout << std::flush;
  return 0;
}


extern "C" int my_io_getevents(io_context_t ctx, long min_nr, long nr, struct io_event *events[],
                    struct timespec *timeout, sigset_t *sigmask) {
  printf("io_getevents:: IO_CONTEXT at %#x size %#x (content: %#x)\n", &ctx, sizeof(ctx), ctx);

  if (timeout) {
    my_timespec_info(timeout);
  }
  return 0;
}
