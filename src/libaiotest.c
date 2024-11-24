#include <aio.h>
#include <stdio.h>
#include <stddef.h>
#include <ctype.h>

int const nprint = 16;

int my_sigevent_info(struct sigevent* ptr) {
  printf("SIGEV at %#x size %d\n", ptr, sizeof(struct sigevent));
  printf("SIGEV->value = @%#x size %d at offs %#x\n",
         &ptr->sigev_value, sizeof(ptr->sigev_value), offsetof(struct sigevent, sigev_value));
  printf("SIGEV->value.sival_int = %d\n", ptr->sigev_value.sival_int);
  printf("SIGEV->value.sival_ptr = @%#x\n", ptr->sigev_value.sival_ptr);
  printf("SIGEV->signo = %d size %d at offs %#x\n",
         ptr->sigev_signo, sizeof(ptr->sigev_signo), offsetof(struct sigevent, sigev_signo));
  printf("SIGEV->notify = %d size %d at offs %#x\n",
         ptr->sigev_notify, sizeof(ptr->sigev_notify), offsetof(struct sigevent, sigev_notify));
  printf("SIGEV->notify_function = @%#x size %d at offs %#x\n",
         ptr->sigev_notify_function, sizeof(ptr->sigev_notify_function), offsetof(struct sigevent, sigev_notify_function));
  return 0;
}

int my_aio_info(struct aiocb* ptr) {
  printf("SIGEV_NONE = %d\n", (int)(SIGEV_NONE));
  printf("SIGEV_SIGNAL = %d\n", (int)(SIGEV_SIGNAL));
  printf("SIGEV_THREAD = %d\n", (int)(SIGEV_THREAD));
  printf("AIOCB at %#x size %d\n", ptr, sizeof(struct aiocb));
  printf("AIOCB->fildes = %d size %d at offs %#x\n", ptr->aio_fildes, sizeof(ptr->aio_fildes), offsetof(struct aiocb, aio_fildes));
  printf("AIOCB->offset = %ld size %d at offs %#x\n", ptr->aio_offset, sizeof(ptr->aio_offset), offsetof(struct aiocb, aio_offset));
  printf("AIOCB->buf = %#x size %d at offs %#x\n", ptr->aio_buf, sizeof(ptr->aio_buf), offsetof(struct aiocb, aio_buf));
  printf("AIOCB->nbytes = %ld size %d at offs %#x\n", ptr->aio_nbytes, sizeof(ptr->aio_nbytes), offsetof(struct aiocb, aio_nbytes));
  printf("AIOCB->sigevent = {@%#x} size %d at offs %#x\n", &ptr->aio_sigevent, sizeof(ptr->aio_sigevent), offsetof(struct aiocb, aio_sigevent));
  if (ptr->aio_nbytes && ptr->aio_buf) {
    printf("AIOCB->buf[1:%d] = ", nprint);
    for (int i = 0; i < nprint && i < ptr->aio_nbytes; ++i) {
      char c = ((char*) ptr->aio_buf)[i];
      if (c != 0 && isascii(c)) {
        printf("%c", c);
      } else {
        printf("\\%02x", c);
      }
    }
    printf("\n");
  }
  my_sigevent_info(&ptr->aio_sigevent);
  return 0;
}
