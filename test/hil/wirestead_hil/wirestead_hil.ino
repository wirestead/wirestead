// Serial peer for wirestead hardware-in-the-loop tests.
//
// Echoes every byte back, except lines that start with '!', which are commands:
//   !ping        -> PONG
//   !boots       -> BOOTS <n>   (EEPROM boot counter; tells whether opening the
//                                port reset the board through DTR)
//   !mute <ms>   -> stays silent and drops input for <ms>, then echoes again
//   !tick <ms>   -> sends T<n> every <ms> until the next command
//   !echo        -> back to plain echo
// Prints "HIL-BOOT <n>" once after every reset.
//
// Flash: arduino-cli compile -b arduino:avr:uno test/hil/wirestead_hil &&
//        arduino-cli upload -b arduino:avr:uno -p <port> test/hil/wirestead_hil

#include <EEPROM.h>

static const unsigned long kBaud = 115200;

enum Mode { ECHO, MUTE, TICK };
static Mode mode = ECHO;
static unsigned long mode_until = 0;
static unsigned long tick_period = 0;
static unsigned long next_tick = 0;
static unsigned long tick_count = 0;
static unsigned long boots = 0;

static bool line_start = true;
static bool in_command = false;
static char cmd[24];
static uint8_t cmd_len = 0;

static void run_command() {
  cmd[cmd_len] = '\0';
  unsigned long arg = 0;
  const char* space = strchr(cmd, ' ');
  if (space) arg = strtoul(space + 1, nullptr, 10);

  if (strncmp(cmd, "ping", 4) == 0) {
    Serial.print("PONG\n");
  } else if (strncmp(cmd, "boots", 5) == 0) {
    Serial.print("BOOTS ");
    Serial.print(boots);
    Serial.print('\n');
  } else if (strncmp(cmd, "mute", 4) == 0) {
    mode = MUTE;
    mode_until = millis() + arg;
  } else if (strncmp(cmd, "tick", 4) == 0 && arg > 0) {
    mode = TICK;
    tick_period = arg;
    tick_count = 0;
    next_tick = millis() + arg;
  } else if (strncmp(cmd, "echo", 4) == 0) {
    mode = ECHO;
  } else {
    Serial.print("ERR\n");
  }
}

void setup() {
  EEPROM.get(0, boots);
  if (boots == 0xFFFFFFFFUL) boots = 0;  // erased EEPROM
  ++boots;
  EEPROM.put(0, boots);

  Serial.begin(kBaud);
  Serial.print("HIL-BOOT ");
  Serial.print(boots);
  Serial.print('\n');
}

void loop() {
  const unsigned long now = millis();

  if (mode == MUTE) {
    while (Serial.available()) Serial.read();
    if ((long)(now - mode_until) >= 0) {
      mode = ECHO;
      line_start = true;
    }
    return;
  }

  if (mode == TICK && (long)(now - next_tick) >= 0) {
    Serial.print('T');
    Serial.print(++tick_count);
    Serial.print('\n');
    next_tick += tick_period;
  }

  while (Serial.available()) {
    const char c = Serial.read();
    if (in_command) {
      if (c == '\n' || c == '\r') {
        in_command = false;
        line_start = true;
        run_command();
      } else if (cmd_len < sizeof(cmd) - 1) {
        cmd[cmd_len++] = c;
      }
      continue;
    }
    if (line_start && c == '!') {
      in_command = true;
      cmd_len = 0;
      continue;
    }
    if (mode == ECHO) Serial.write(c);
    line_start = (c == '\n');
  }
}
