#define LOOPS 30000000

int main(int argc, char* argv[])
{
  for (int i = 0; i < LOOPS; i++){
    argc += i;
  }
  return argc == 0;
}

void _start(int argc, char* argv[])
{
  register long a0 asm("a0") = main(argc, argv);
  register long syscall_id asm("a7") = 93;

  asm volatile ("scall" : "+r"(a0) : "r"(syscall_id));
}
