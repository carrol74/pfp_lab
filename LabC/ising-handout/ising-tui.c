#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <termios.h>
#include "ising.h"

struct termios orig_termios;

void cooked_mode() {
  tcsetattr(STDIN_FILENO, TCSAFLUSH, &orig_termios);
  printf("\e[?25h");
}

void raw_mode() {
  printf("\e[?25l");

  tcgetattr(STDIN_FILENO, &orig_termios);
  atexit(cooked_mode);

  struct termios raw = orig_termios;
  raw.c_iflag &= ~(IXON);
  raw.c_lflag &= ~(ECHO | ICANON | ISIG);
  raw.c_cc[VMIN] = 0;
  raw.c_cc[VTIME] = 0;
  tcsetattr(STDIN_FILENO, TCSAFLUSH, &raw);
}

void fg_rgb(uint8_t r, uint8_t g, uint8_t b) {
  printf("\033[38;2;%d;%d;%dm", r, g, b);
}

void bg_rgb(uint8_t r, uint8_t g, uint8_t b) {
  printf("\033[48;2;%d;%d;%dm", r, g, b);
}

void def() {
  printf("\e[39;49m");
}

void clear_line() {
  printf("%c[2K", 27);
}

void futhark_check(struct futhark_context *ctx, int err, const char* file, int line) {
  if (err != 0) {
    fprintf(stderr, "%s:%d:\n", file, line);
    fprintf(stderr, "%s\n", futhark_context_get_error(ctx));
    exit(1);
  }
}

#define FUTHARK_CHECK(ctx,err) futhark_check(ctx,err,__FILE__, __LINE__)

void render_state(struct futhark_context *ctx, struct futhark_opaque_state *state) {
  struct futhark_i8_2d *screen_arr;
  int err;

  err = futhark_entry_tui_render(ctx, &screen_arr, state);
  FUTHARK_CHECK(ctx,err);

  int height = futhark_shape_i8_2d(ctx, screen_arr)[0];
  int width = futhark_shape_i8_2d(ctx, screen_arr)[1];

  int8_t *pixels = malloc(height*width*sizeof(int8_t));

  futhark_values_i8_2d(ctx, screen_arr, pixels);
  assert(err == 0);

  for (int i = 0; i < height/2; i++) {
    for (int j = 0; j < width; j++) {
      uint32_t p0 = pixels[(i*2)*width+j];
      uint32_t p1 = pixels[(i*2+1)*width+j];
      if (p0 == 1) {
        fg_rgb(0,0,0xFF);
      } else {
        fg_rgb(0xFF,0,0);
      }
      if (p1 == 1) {
        bg_rgb(0,0,0xFF);
      } else {
        bg_rgb(0xFF,0,0);
      }
      fputs("▀", stdout);
    }
    fputs("\n", stdout);
  }
  def();
  fflush(stdout);
  free(pixels);
}

int main() {
  struct winsize w;
  ioctl(STDOUT_FILENO, TIOCGWINSZ, &w);

  int height = w.ws_row-1;
  int width = w.ws_col;
  int err;

  struct futhark_context_config *cfg = futhark_context_config_new();
  assert(cfg != NULL);

#ifdef FUTHARK_BACKEND_opencl
  futhark_context_config_select_device_interactively(cfg);
#endif

  struct futhark_context *ctx = futhark_context_new(cfg);
  assert(ctx != NULL);

  struct futhark_opaque_state *state;
  uint32_t seed = 123;

  err = futhark_entry_tui_init(ctx, &state, seed, height*2, width);
  FUTHARK_CHECK(ctx,err);

  int buffer_size = height*2*(width+1);
  char buffer[height*2*(width+1)*10];

  raw_mode();
  setvbuf(stdout, buffer, _IOFBF, buffer_size);

  float abs_temp = 0.5;
  float samplerate = 0.1;

  printf("\033[2"); // Clear screen.
  while (1) {
    render_state(ctx, state);

    struct futhark_opaque_state *new_state;
    err = futhark_entry_tui_step(ctx, &new_state, abs_temp, samplerate, state);
    FUTHARK_CHECK(ctx,err);

    err = futhark_free_opaque_state(ctx, state);
    FUTHARK_CHECK(ctx,err);

    state = new_state;

    char c;
    if (read(STDIN_FILENO, &c, 1) != 0) {
      if (c == 'q') {
        break;
      } else if (c == 'z') {
        samplerate *= 0.99;
      } else if (c == 'x') {
        samplerate *= 1.01;
      } else if (c == 'a') {
        abs_temp *= 0.99;
      } else if (c == 's') {
        abs_temp *= 1.01;
      }
    }
    clear_line();
    printf(" temp (a/s): %.3f        samplerate (z/x): %.3f        quit (q)", abs_temp, samplerate);
    fflush(stdout);
    printf("\r\033[%dA", height); // Move up.
  }
  printf("\n");

  futhark_context_free(ctx);
  futhark_context_config_free(cfg);
}
