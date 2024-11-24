#include <aio.h>
#include <stdio.h>
#include <stddef.h>
#include <ctype.h>

int const nprint = 16;

int my_aio_info(struct aiocb* ptr) {
  printf("AIOCB at %#x size %d\n", ptr, sizeof(struct aiocb));
  printf("AIOCB->filedes = %d size %d at offs %#x\n", ptr->aio_fildes, sizeof(ptr->aio_fildes), offsetof(struct aiocb, aio_fildes));
  printf("AIOCB->offset = %d size %d at offs %#x\n", ptr->aio_offset, sizeof(ptr->aio_offset), offsetof(struct aiocb, aio_offset));
  printf("AIOCB->buf = %#x size %d at offs %#x\n", ptr->aio_buf, sizeof(ptr->aio_buf), offsetof(struct aiocb, aio_buf));
  printf("AIOCB->nbytes = %d size %d at offs %#x\n", ptr->aio_nbytes, sizeof(ptr->aio_nbytes), offsetof(struct aiocb, aio_nbytes));
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
  return 0;
}
