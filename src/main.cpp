// ==========================================================================
// Ajeevi / Modbus Industrial Gateway  —  v3.5.0 (production)
// Modbus TCP (W5500 Ethernet)  <->  Modbus RTU (RS-485)  transparent bridge
// Target: ESP32 + W5500, Arduino core 2.0.x / 3.x, Arduino Ethernet >= 2.0.2
// ==========================================================================
//
// YE GATEWAY KIS LIYE HAI?
// ------------------------
// Ye ek protocol converter / translator hai: Ethernet network aur purane
// RS-485 industrial devices ke beech "dubhashiya".
//
//   SCADA PC --Ethernet (Modbus TCP)--> [ESP32 + W5500] --RS-485 (Modbus RTU)--> Meter/VFD/PLC
//            <------- response -------                 <------- response -------
//
//  * Field devices (energy meters, VFD, PLC, temp controllers, solar
//    inverters, flow meters) Modbus RTU bolte hain, RS-485 ki 2 wires par.
//    Ye LAN par nahi chalta, aur ek time par sirf EK master baat kar sakta hai.
//  * Control room ka SCADA / HMI (WinCC, Ignition, Wonderware) ya koi
//    Node.js / Python dashboard Modbus TCP bolta hai (Ethernet, port 502).
//
//  Flow:
//   1. SCADA gateway ke IP (e.g. 192.168.1.50:502) par TCP request bhejta hai
//      ("unit 5 ke register 100-110 padho").
//   2. Gateway MBAP header hata ke RTU frame banata hai, CRC jodta hai, aur
//      RS-485 bus par bhejta hai.
//   3. Device ka jawab aata hai -> CRC check -> wapas TCP format -> SCADA.
//   4. Device jawab na de to SCADA ko exception 0x0B milta hai
//      ("gateway target device failed to respond").
//  Gateway data ka matlab nahi samajhta, sirf format badalta hai
//  ("transparent bridge").
//
//  Use cases: building ke energy meters office LAN se padhna, solar plant ka
//  inverter data SCADA/cloud par, purani RS-485 machines ko naye Ethernet
//  network se jodna, aur 4 SCADA/HMI clients ko ek hi RTU bus share karwana
//  (gateway requests ko queue karke ek-ek karke bhejta hai).
//  Commercial equivalent: Moxa MGate MB3180 jaise gateways.
//
// BUILD (PlatformIO) — platformio.ini:
//   [env:esp32dev]
//   platform      = espressif32
//   board         = esp32dev
//   framework     = arduino
//   monitor_speed = 115200
//   lib_deps      = arduino-libraries/Ethernet@^2.0.2
//   build_flags   = -DCORE_DEBUG_LEVEL=0 -Wall -Wextra -Wno-unused-parameter
// Arduino IDE: file ko "ModbusGateway/ModbusGateway.ino" naam se save karein,
// board "ESP32 Dev Module", Library Manager se "Ethernet" (>= 2.0.2).
// Tested compile: arduino-esp32 core 2.0.17 aur 3.3.12, Ethernet 2.0.2.
//
// ARCHITECTURE (why it is shaped like this)
// -----------------------------------------
//  * ONE task ("NetTask") owns the W5500 / SPI bus / Ethernet library. Modbus
//    TCP server, HTTP config server, link polling and W5500 health checks all
//    run inside it. Nobody else ever calls an Ethernet.* / EthernetClient /
//    EthernetServer API. The Arduino Ethernet library keeps global socket
//    state and is NOT thread-safe, so single ownership is the only design
//    that is correct by construction (a mutex around "SPI" is not enough —
//    the library's own socket bookkeeping would still race).
//  * ONE task ("RtuTask", other core) owns the RS-485 UART. It is the only
//    code that waits for a meter's reply. NetTask hands it a job through a
//    FreeRTOS queue and polls for the result with zero timeout, so a slow or
//    dead meter (up to the RTU timeout) never freezes TCP, HTTP, new SCADA
//    connections, link polling or the W5500 health check. The one remaining
//    place where NetTask can wait on a peer is writeAll(), and that wait has
//    a hard deadline of TCP_WRITE_TIMEOUT_MS for the whole transfer.
//  * loop() only drives LEDs and the external watchdog. It reads 32-bit
//    volatile values published by the tasks — no SPI / UART access at all.
//  * Serial side: Modbus RTU only. Modbus ASCII is NOT supported (a MOXA
//    MGate MB3180 supports both) - a capability difference, not a defect.
//  * NO RETRIES. A failed transaction is reported to the master, never
//    repeated by the gateway: silently retrying a write function (05/06/0F/10)
//    could apply it twice if only the response was lost. Retry policy belongs
//    to the master, which knows whether its request is idempotent.
//  * No heap use in the request path: servers are static globals, every buffer
//    is a fixed-size static array, the HTTP server has its own zero-alloc
//    parser. (The two job queues are allocated once at boot, and the NVS layer
//    allocates while an operator saves the configuration - neither is in the
//    Modbus or HTTP data path.)
//  * The heartbeats prove that each task is still looping. Progress is checked
//    separately: an RTU job that does not come back within its deadline is
//    detected and, if it keeps happening, the device restarts itself.
//
// FIXES vs previous draft (numbers match the chat summary)
//  [1] server.available() -> server.accept(): each new connection is handed
//      over exactly once, plus socket-number de-duplication.        (SCADA drop)
//  [2] Single-owner W5500 (see above). Old draft touched SPI from core 0
//      (Modbus task) and core 1 (Ethernet.linkStatus() in loop()).  (panic)
//  [3] RS-485 DE released only after uart_wait_tx_done() (last STOP bit
//      physically left the shift register) + 2-bit guard. Optional hardware
//      RS485 half-duplex mode (UART drives DE itself).               (CRC cut)
//  [4] `new CustomEthernetServer` removed -> static global objects. (heap)
//  Hidden bugs also fixed:
//  [5] WebServer.h is the *WiFi* web server -> config page was never
//      reachable over W5500. Replaced by an Ethernet HTTP server on port 80.
//  [6] Partial MBAP header was consumed then "retried" -> TCP stream desync.
//      Now a per-client non-blocking reassembly buffer (also handles
//      pipelined requests and never lets one slow client stall the others).
//  [7] Half-open sockets (cable pulled / SCADA PC crash) held slots forever.
//      Idle timeout + evict-oldest when all slots are busy.
//  [8] Stale slot aliasing: a dead slot's socket number can be reused by the
//      listener for a NEW connection -> two slots on one socket / cross-talk
//      between the Modbus and HTTP servers. Socket ownership reconciliation.
//  [9] EthernetClient::write() silently truncates at the W5500 socket buffer
//      (2 KB) and can spin for tens of seconds on a dead peer (W5500 retry
//      back-off doubles each time: 200ms x 8 retries). Chunked, bounded
//      writes + shorter retransmission settings.
// [10] EthernetClient::stop() blocks up to 1000 ms -> now 100 ms.
// [11] RTU RX: ESP32 UART only pushes bytes to the ring buffer every 120
//      bytes or after a 10-symbol idle. At 9600 baud that is 11.5 ms > the
//      old 10 ms gap -> long frames were cut and failed CRC. FIFO threshold
//      set to 1, and end-of-frame is decided from the Modbus function code
//      (exact expected length) with a baud-derived t3.5 gap as fallback.
//      Old loop also read only 1 byte per 1 ms tick.
// [12] Response not checked against request (unit id / function code) ->
//      a late reply from a previous timed-out poll was forwarded to SCADA.
// [13] Broadcast (unit 0) waited 1 s and returned a bogus 0x0B exception.
// [14] No inter-frame t3.5 silence enforced between consecutive RTU frames.
// [15] External watchdog was fed from inside blocking wait loops (so a hung
//      task could never be detected). Now fed by a supervisor only while
//      NetTask proves it is alive; NetTask is also on the ESP task WDT.
// [16] W5500 lock-up / spontaneous reset (EMI) was never detected. Periodic
//      register sanity check + automatic W5500 re-initialisation.
// [17] HTML template contained "100%;" inside a printf format string (UB).
// [18] NVS values not validated at boot (corrupt baud -> UART failure),
//      Modbus port 80 would collide with the web server, subnet mask /
//      gateway not sanity-checked, MAC could get multicast bit.
// [19] Basic-auth brute force: lockout after repeated failures.
// v2.2.0 audit fixes (FIX-01 .. FIX-28 of the v2.1.0 production audit):
// [21] RTU frame boundary: reaching the expected byte count no longer ends the
//      frame on its own — a t3.5 silence must follow and the length must match
//      exactly. Extra bytes are a FRAME ERROR now, never silently truncated.
// [22] RTU errors are classified: TIMEOUT / CRC / FRAME / UNIT MISMATCH /
//      FC MISMATCH, each with its own counter, each mapped to a different
//      Modbus exception (0x0B / 0x04 / 0x0A) instead of everything being 0x0B.
// [23] Requests are validated before they reach the bus (illegal function ->
//      0x01, impossible length/quantity -> 0x03), so a broken client cannot
//      occupy the RS-485 line with frames no slave can answer.
// [24] Configuration is stored as ONE versioned CRC-protected blob -> a power
//      cut during save can no longer leave a half-new configuration.
// [25] Eviction only removes sessions idle beyond EVICT_IDLE_THRESHOLD_MS and
//      never one with a request in flight; otherwise the new connection is
//      refused (a busy SCADA link is no longer dropped for a newcomer).
// [26] Web password: per-device default derived from the MAC, SHA-256 hash in
//      NVS, change-password form, per-IP lockout, CSRF token on /save.
// [27] writeAll() has an absolute deadline; a stalled peer cannot hold NetTask.
// [28] Lost RTU job detection, sliced TX wait (watchdog-safe at 1200 baud),
//      stronger W5500 health check (live MAC register) with a 3-strike filter,
//      UART framing/parity/overflow counters, stack/heap diagnostics.
//
// v2.3.0 audit-#2 fixes (MOXA MGate MB3180 used as the behavioural reference):
// [29] The t3.5 silence window no longer busy-waits: above 3 ms (i.e. at
//      <= 19200 baud, where t3.5 reaches 32 ms at 1200) the task yields
//      instead of spinning on the UART lock.
// [30] t3.5 is charged ONCE per transaction: the silence just observed at the
//      frame boundary IS the inter-frame gap.
// [31] Separate write deadlines: 500 ms for a Modbus response, 3 s for the
//      (much larger) web page, so a slow browser cannot truncate the page and
//      a stalled Modbus peer still cannot hold the network task.
// [32] Configuration save is all-or-nothing across BOTH stores: if the
//      password write fails, the configuration blob is rolled back.
// [33] Brute force: per-IP lockout PLUS a global failed-login rate cap, so
//      rotating source addresses no longer bypass it.
// [34] MOXA-style switch: "return a Modbus exception when the slave does not
//      answer" can be turned off (some masters prefer silence and their own
//      timeout). Applies to RTU failures, not to locally rejected requests.
// [35] Measured slave turnaround (min/max) recorded and shown with a suggested
//      response timeout - the manual equivalent of MOXA's "Auto Detection".
// [36] Per-unit-id success/failure counters, so one bad slave can be found
//      without a bus analyser.
// [37] Legacy NVS keys removed after migration; strict request validation can
//      be relaxed for vendor-specific frames; /config.json export endpoint.
//
// v3.0.0 — universal-gateway hardening (audit #3). The TCP side makes NO
// assumption about who the master is: SCADA, PLC, HMI, BMS, EMS, DCS, a Python
// or Node client, another gateway — all are ordinary Modbus TCP clients.
// [38] RTU JOB OWNERSHIP (was a real stale-result race): the shared job buffer
//      is never rewritten while RtuTask may still own it. Every job carries a
//      unique 32-bit id; the completion queue carries that id; an overdue job
//      moves to an ABANDONED state where its late result is recognised and
//      discarded instead of being taken for the next request's answer.
// [39] PER-CLIENT REQUEST QUEUES: each TCP client may pipeline up to
//      REQ_QUEUE_DEPTH requests. Requests are parsed out of the byte stream
//      into a per-client ring, dispatched round-robin, and answered in order
//      on the connection they came from. Transaction ids are scoped to their
//      connection, never compared globally. A full ring simply stops draining
//      the socket (TCP back-pressure) instead of dropping anything.
// [40] Queued requests age out (REQUEST_MAX_AGE_MS) with exception 0x0A rather
//      than occupying the bus long after the master has given up.
// [41] ATOMIC PERSISTENCE: network settings, serial settings, gateway options,
//      password hash and the default-password flag now live in ONE versioned,
//      CRC-protected blob written to A/B slots with a generation counter and a
//      read-back verify. Power loss at any instant leaves exactly one valid
//      slot; config and password can no longer disagree.
// [42] t1.5 inter-character gap detection (advisory above 19200 baud, where the
//      gap is shorter than the scheduler's resolution — see RTU_GAP_FLOOR_US).
// [43] The serial log no longer prints the default password; it says only that
//      the device default is active. The password itself is on the label.
//
// v3.1.0 fixes (audit #4):
// [44] CRITICAL, initialisation order: loadConfig() derives the per-device
//      default password from mac[], but generateUniqueMac() ran AFTER it. On a
//      fresh device (or a legacy migration) the stored hash was therefore
//      computed from 00:00:00:00:00:00 while the label-derived password was the
//      real one -> nobody could log in. The MAC is now produced before any
//      persistence code runs, and a device already flashed with the broken
//      order heals itself on the next boot (see loadConfig).
// [45] CSRF: a ring of four tokens, each with its own expiry, replaces the
//      single global token, so several open configuration tabs all stay usable.
//      Tokens are still unpredictable, single-use and expiring.
// [46] After an abandoned RTU job the UART is explicitly re-synchronised
//      (drain to silence) before the next transaction is allowed on the bus.
// [47] Watchdog ladder documented and widened: a slow slave or a normal Modbus
//      timeout can never reboot the device; only a genuinely stuck task can.
// [48] HTTP: real HEAD support, shorter write deadline so a slow browser cannot
//      delay Modbus, and write failures are detected and counted.
// [49] Eviction never touches a client that has queued requests.
// [50] Diagnostics: boot/reset reason plus counters for disconnects, queue
//      back-pressure, abandoned jobs, late completions, auth failures and
//      lockouts, and configuration write/CRC failures.
//
// v3.2.0 — W5500 bring-up hardening and diagnostics (field issue: "W5500 not
// detected" on hardware that works with other firmware):
// [51] The chip is now probed DIRECTLY over SPI (VERSIONR at 0x0039 must read
//      0x04) before the Ethernet library is asked to detect it, and the result
//      is logged. That separates "SPI/wiring/speed problem" from "library
//      detection problem" in one line of console output.
// [52] If detection fails, the firmware sweeps 1/2/4/8/14/20 MHz with the raw
//      probe and prints which clocks answer, because the Arduino Ethernet
//      library talks to the W5500 at a fixed 14 MHz that long jumper wires or
//      a breadboard often cannot sustain.
// [53] Init is retried (hardware reset between attempts) instead of giving up
//      after one try, with the external watchdog fed between attempts.
// [54] The hardware reset pin is optional now (W5500_RST -1) for boards that
//      tie RESET to a supervisor chip instead of a GPIO.
// [55] The health check no longer compares the MAC register; it uses the raw
//      VERSIONR probe plus SIPR/SUBR, logs expected-vs-actual on the first
//      mismatch, and cannot re-initialise more often than once per 30 s.
//
// v3.3.0 — the W5500 was held in hardware reset (field bug, found by comparing
// with the factory test firmware that worked on the same PCB):
// [56] RESET POLARITY. On this PCB the ESP32 pin drives a TRANSISTOR, so a HIGH
//      on the GPIO pulls the W5500 /RESET pin LOW (reset asserted) and a LOW
//      releases it through the pull-up. The firmware did the opposite and left
//      the GPIO high, i.e. the chip stayed in reset for ever and every register
//      read returned 0x00 at every SPI clock. Polarity is now an explicit,
//      documented option (W5500_RST_ASSERT_HIGH).
// [57] After releasing reset the firmware polls VERSIONR until the chip answers
//      (up to W5500_BOOT_TIMEOUT_MS) and logs the measured boot time, instead
//      of assuming a fixed delay: a weak 25 MHz crystal shows up here.
// [58] SPI.begin() is called with SS = -1 so the ESP32 does not route a hardware
//      CS onto GPIO5 while the Ethernet library drives the same pin by hand.
// [59] The raw probe runs at 1 MHz (the factory test's proven speed for these
//      PCB traces) - it is a diagnostic, it does not need to be fast.
// [60] First boot no longer prints the scary "nvs_get_blob len fail" error: the
//      slot is checked with isKey() before it is read.
//
// v3.4.0 — commissioning login:
// [61] The web password is a plain, known value again (admin / admin123 by
//      default) because the MAC-derived one is impossible to guess in the field
//      without the label. WEB_FORCE_DEFAULT_PASSWORD keeps it in force at every
//      boot, so the device can never lock you out. Set that option to 0 (and
//      change the password from the web page) before shipping: while it is 1,
//      every unit has the same password and the console prints it.
//
// v3.5.0 — "500 Internal Error / Page buffer overflow" on the config page:
// [62] The status page grew past the 4 kB build buffer as diagnostics were
//      added. The buffer is now 12 kB (static, still no heap), the real page
//      size is logged at boot, and an overflow reports how many bytes were
//      needed instead of just saying "overflow".
//
// [20] Synchronous serial wait: the RTU response wait used to run inside the
//      network task (up to rtuTimeoutMs, 5 s max) -> web page, accept(),
//      other clients' reads and W5500 health checks froze meanwhile. Now an
//      asynchronous job queue to a dedicated RtuTask; NetTask never blocks
//      on RS-485. Late results for a client that disconnected are dropped
//      (generation counter), and replies stay in request order per client.
// ==========================================================================

#include <Arduino.h>
#include <SPI.h>
#include <Ethernet.h>
#include <Preferences.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <ctype.h>
#include "esp_mac.h"
#include "esp_task_wdt.h"
#include "driver/uart.h"
#include "mbedtls/base64.h"
#include "esp_random.h"
#include "esp_system.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"

// ==========================================================================
// 1. ESP32 ABSTRACT CLASS FIX (WRAPPER)
// ==========================================================================
// Core 2.x: Server has pure virtual begin(uint16_t) but the Ethernet lib only
// provides begin() -> EthernetServer is abstract; this wrapper fixes that.
// Core 3.x: Server::begin() matches the Ethernet lib already, so the wrapper
// must NOT override anything (an `override` there is a compile error).
#include "esp_arduino_version.h"
#ifndef ESP_ARDUINO_VERSION_MAJOR
#define ESP_ARDUINO_VERSION_MAJOR 2
#endif
class CustomEthernetServer : public EthernetServer {
  public:
    explicit CustomEthernetServer(uint16_t port) : EthernetServer(port) {}
#if ESP_ARDUINO_VERSION_MAJOR < 3
    void begin(uint16_t port = 0) override { (void)port; EthernetServer::begin(); }
#endif
};

// ==========================================================================
// 2. PIN DEFINITIONS & TUNABLES
// ==========================================================================
#define FW_VERSION "3.5.0"

// W5500 Ethernet
#define W5500_MOSI 23
#define W5500_MISO 19
#define W5500_SCLK 18
#define W5500_CS   5
// Set to -1 if RESET is not wired to a GPIO on your board (a supervisor chip
// or an RC network drives it). The library still performs a software reset.
#ifndef W5500_RST
#define W5500_RST  25
#endif

// RESET POLARITY — get this wrong and the chip never runs (audit FIX-56).
//   1 = the GPIO drives an inverting stage (transistor): GPIO HIGH asserts the
//       W5500 reset, GPIO LOW releases it through the board's pull-up.
//       This is the Ajeevi PCB.
//   0 = the GPIO goes straight to the W5500 /RESET pin: LOW asserts reset.
#ifndef W5500_RST_ASSERT_HIGH
#define W5500_RST_ASSERT_HIGH 1
#endif
#define W5500_RST_ASSERT_LEVEL  (W5500_RST_ASSERT_HIGH ? HIGH : LOW)
#define W5500_RST_RELEASE_LEVEL (W5500_RST_ASSERT_HIGH ? LOW  : HIGH)
#define W5500_RST_PULSE_MS 250                // factory test measured this as reliable
#define W5500_BOOT_TIMEOUT_MS 1500            // crystal + PLL + internal boot

// RS-485 UART (UART2)
#define RXD2 16
#define TXD2 17
#define RS485_UART_NUM UART_NUM_2

// RS-485 direction control (DE and /RE tied together).
//  -1  : transceiver has automatic direction control (no DE pin used)
//  >=0 : GPIO driving DE/RE
#ifndef RS485_DE_PIN
#define RS485_DE_PIN -1
#endif
//  0 : firmware drives DE (waits for TX-complete, then releases)       [default]
//  1 : UART hardware drives DE via its RTS line (UART_MODE_RS485_HALF_DUPLEX).
//      Most precise option; requires RS485_DE_PIN >= 0.
#ifndef RS485_USE_HW_DE
#define RS485_USE_HW_DE 0
#endif
#define RS485_DE_SETUP_US 10   // DE asserted -> first start bit

// Status LEDs & external watchdog (DONE pin, e.g. TPL5010)
//
// !! HARDWARE WARNING (audit FIX-01) !!
// GPIO12 is the MTDI strapping pin: it selects the flash voltage at reset and
// MUST be below ~0.8 V while the chip comes out of reset. An LED circuit that
// can pull it high at power-up makes the board boot with 1.8 V flash settings
// -> intermittent or total boot failure in the field. Verify with a scope. If
// the PCB cannot be changed, burn the flash voltage eFuse
// (espefuse.py set_flash_voltage 3.3V) during production programming, or move
// the LED to a non-strapping GPIO (13, 26, 27, 32, 33 are safe here).
// ESP32 strapping pins: 0, 2, 4, 5, 12, 15.
#define LED_D3 14
#define LED_D4 12
#define WDT_DONE_PIN 33

// Optional factory reset: hold this pin LOW during power-up for
// FACTORY_RESET_HOLD_MS to wipe saved config. -1 = disabled.
#ifndef FACTORY_RESET_PIN
#define FACTORY_RESET_PIN -1
#endif
#define FACTORY_RESET_HOLD_MS 5000

// Modbus TCP
#define MAX_CLIENTS 4
#define TCP_PARTIAL_FRAME_TIMEOUT_MS 1000UL   // incomplete ADU -> drop client
#define CLIENT_IDLE_TIMEOUT_MS 300000UL       // no traffic -> close (0 = never)
#ifndef EVICT_OLDEST_WHEN_FULL
#define EVICT_OLDEST_WHEN_FULL 1              // only STALE sessions may be evicted (see below)
#endif
#define EVICT_IDLE_THRESHOLD_MS 30000UL       // a session must be silent this long to be evictable
#define SOCKET_CLOSE_TIMEOUT_MS 100           // EthernetClient::stop() wait
#define TCP_WRITE_TIMEOUT_MS 500UL            // one Modbus response (<= 260 bytes)
#define HTTP_WRITE_TIMEOUT_MS 1500UL          // page write must not delay Modbus (FIX-08)

// W5500 TCP retransmission: RTR doubles each retry -> 100+200+400+800 = 1.5 s
#define W5500_RETX_TIMEOUT_MS 100
#define W5500_RETX_COUNT 3

// Modbus RTU
#define RS485_DEFAULT_TIMEOUT_MS 1000
#define RS485_MIN_GAP_MS 5                    // floor for silence-based end-of-frame
#define BROADCAST_TURNAROUND_MS 100
#define RS485_RX_BUFFER 1024

// HTTP config server
#define HTTP_PORT 80
#define HTTP_BUF_SIZE 1536
#define HTTP_REQUEST_TIMEOUT_MS 3000UL
#define AUTH_MAX_FAILS 5
#define AUTH_LOCKOUT_MS 30000UL
#define AUTH_TRACKED_HOSTS 4                  // per-source-IP lockout table
#define CSRF_TOKEN_TTL_MS 300000UL
#define CSRF_TOKEN_SLOTS 4                    // several config tabs may be open at once

// Optional: only these hosts may open a Modbus TCP connection (0.0.0.0 = unused).
// Modbus TCP has no authentication of its own; this is a crude but effective
// second line of defence when the gateway cannot be put on an isolated VLAN.
#ifndef MODBUS_ALLOWLIST_ENABLED
#define MODBUS_ALLOWLIST_ENABLED 0
#endif
#define MODBUS_ALLOWLIST { IPAddress(0,0,0,0), IPAddress(0,0,0,0) }

// Supervision
#define LINK_POLL_MS 250UL
#define HEALTH_CHECK_MS 5000UL
#define NET_HEARTBEAT_MAX_AGE_MS 3000UL       // NetTask silent longer -> starve ext. WDT
#define RTU_JOB_GRACE_MS 3000UL               // job overdue by this much => RtuTask is gone
// Watchdog ladder (audit FIX-04). These three layers must never overlap:
//
//   a slow slave / normal Modbus timeout  -> handled inside RtuTask, a result
//                                            always comes back: NO reboot
//   no completion at all after the timeout -> grace period, then ABANDONED and
//     + RTU_JOB_GRACE_MS                     the client gets exception 0x0A
//   RTU_JOB_LOST_LIMIT abandoned jobs      -> RtuTask is stuck: controlled
//                                             esp_restart()
//   NetTask silent > NET_HEARTBEAT_MAX_AGE  -> external watchdog stops being fed
//   RtuTask silent > that + rtuTimeout + 1s -> external watchdog stops being fed
//
// Worst case before the self-restart: RTU_JOB_LOST_LIMIT x (rtuTimeoutMs +
// RTU_JOB_GRACE_MS) ~= 12 s with the default timeout. The external watchdog
// window must be longer than that, otherwise it fires first.
#define RTU_JOB_LOST_LIMIT 3                  // that many lost jobs -> controlled restart
#define ETH_HEALTH_STRIKES 3                  // consecutive bad reads before re-initialising
#define ETH_INIT_ATTEMPTS 3                   // detection attempts per initialisation
#define ETH_RECOVERY_MIN_INTERVAL_MS 30000UL  // never reinitialise more often than this
#define W5500_VERSIONR 0x0039                 // common register: always reads 0x04
#define W5500_EXPECTED_VERSION 0x04
#define ETH_PROBE_HZ 1000000UL                // probe clock: slow on purpose, bare PCB traces
#define EXT_WDT_FEED_MS 1000UL
#define NET_TASK_STACK 8192
#define RTU_TASK_STACK 4096
#define RTU_TASK_PRIORITY 2

// 1 = enforce function-specific request lengths and quantity limits.
// 0 = reject only impossible function codes and let vendor-specific uses of
//     standard function codes through (a transparent gateway is expected to
//     pass "valid but uncommon" traffic). Rejections are counted either way.
#ifndef STRICT_REQUEST_VALIDATION
#define STRICT_REQUEST_VALIDATION 1
#endif

#define UNIT_STAT_SLOTS 8

// Per-client pipelining. A Modbus TCP master may legitimately have several
// requests outstanding on one connection; the RS-485 bus stays serialized.
// RAM arithmetic before changing these: each PendingReq is ~268 B, so one
// client costs REQ_QUEUE_DEPTH*268 + 260 (reassembly) ~= 1.1 kB, and four
// clients ~4.4 kB of static RAM. The hard ceiling is the W5500's 8 sockets
// (see the socket arithmetic above), which allows MAX_CLIENTS <= 5.
#define REQ_QUEUE_DEPTH 3                     // requests buffered per client
#define REQUEST_MAX_AGE_MS 5000UL             // queued longer than this -> 0x0A

// Inter-character gap check. Below ~1 ms the observed gap is dominated by task
// scheduling rather than by the wire, so the check uses a floor: above 19200
// baud (t1.5 = 750 us) it is effectively disabled and the CRC plus the exact
// length and t3.5 checks carry the frame. Documented robustness gap.
#ifndef RTU_GAP_CHECK
#define RTU_GAP_CHECK 1
#endif
#define RTU_GAP_FLOOR_US 3000UL                     // per-unit-id counters on the status page

#ifndef DEBUG_SERIAL
#define DEBUG_SERIAL 1
#endif

// Web login. The PASSWORD is not a constant any more: every device derives its
// own default from its MAC ("MG-xxxxxx", printed on the console at boot) and
// stores a SHA-256 hash in NVS. Change it from the web page; a factory reset
// restores the per-device default.
static const char* WEB_AUTH_USER = "admin";

// Commissioning password. Leave WEB_DEFAULT_PASSWORD empty ("") to go back to
// the per-device MAC-derived default ("MG-xxxxxx", printed on the label).
#ifndef WEB_DEFAULT_PASSWORD
#define WEB_DEFAULT_PASSWORD "admin123"
#endif

// 1 = re-apply the default password at EVERY boot: you can always log in, even
//     after a wrong password change or a half-written record. Console prints it.
// 0 = production: the stored password wins, and the default is used only when
//     nothing has been stored yet. Nothing is printed.
#ifndef WEB_FORCE_DEFAULT_PASSWORD
#define WEB_FORCE_DEFAULT_PASSWORD 1
#endif

#define WEB_PASS_MIN_LEN 5
#define WEB_PASS_MAX_LEN 32

// ---- compile-time sanity ----
// Socket arithmetic (audit FIX-A14): the W5500 has 8 hardware sockets. This
// firmware uses 4 Modbus clients + 1 Modbus listener + 1 HTTP listener + 1 HTTP
// client = 7, so MAX_CLIENTS can go to 5 at most. A MOXA MGate MB3180 accepts
// 16 simultaneous TCP masters; matching that is not a firmware change - it needs
// a MAC/PHY with a real TCP stack (ESP32 EMAC + LAN8720 + lwIP) instead of the
// W5500's fixed socket set.
static_assert(MAX_SOCK_NUM >= MAX_CLIENTS + 3,
              "Need sockets for: Modbus clients + Modbus listener + HTTP listener + HTTP client");
#if RS485_USE_HW_DE && (RS485_DE_PIN < 0)
#error "RS485_USE_HW_DE requires RS485_DE_PIN >= 0"
#endif

#if DEBUG_SERIAL
  #define LOGF(...) Serial.printf(__VA_ARGS__)
#else
  #define LOGF(...) do {} while (0)
#endif

// W5500 socket status register values (Sn_SR)
namespace SockSt {
  constexpr uint8_t CLOSED      = 0x00;
  constexpr uint8_t INIT        = 0x13;
  constexpr uint8_t LISTEN      = 0x14;
  constexpr uint8_t SYNSENT     = 0x15;
  constexpr uint8_t SYNRECV     = 0x16;
  constexpr uint8_t ESTABLISHED = 0x17;
  constexpr uint8_t CLOSE_WAIT  = 0x1C;
}

static constexpr size_t MB_TCP_ADU_MAX = 260;  // 7 MBAP + 253 PDU
static constexpr size_t MB_RTU_MAX     = 256;  // 1 unit + 253 PDU + 2 CRC

// ==========================================================================
// 3. GLOBALS (all static storage — no heap after boot)
// ==========================================================================
static Preferences prefs;
static CustomEthernetServer modbusServer(502);        // real port applied in setup()
static CustomEthernetServer httpServer(HTTP_PORT);
static HardwareSerial& rs485 = Serial2;               // UART2 (avoid a 2nd object on same UART)
static TaskHandle_t g_netTask = nullptr;
static TaskHandle_t g_rtuTask = nullptr;
static QueueHandle_t g_rtuJobQ  = nullptr;             // NetTask -> RtuTask  ("job ready")
static QueueHandle_t g_rtuDoneQ = nullptr;             // RtuTask -> NetTask  ("job done")

// cfg.flags bits
#define CFG_FLAG_TCP_EXCEPTION 0x01   // answer RTU failures with a Modbus exception

struct GatewayConfig {
  IPAddress ip, sn, gw;
  uint16_t  port;
  uint32_t  baud;
  uint8_t   fmt;           // index into SERIAL_FMTS
  uint8_t   flags;
  uint16_t  rtuTimeoutMs;
};
static GatewayConfig cfg;
static byte mac[6];

// Supported serial subset (audit FIX-A13): 1200-115200 baud, 8 data bits,
// none/even/odd parity, 1-2 stop bits, no flow control. A MOXA MGate MB3180
// additionally offers 50 bps-921.6 kbps, 7 data bits, space/mark parity and
// RTS/CTS - none of which are common in Modbus RTU installations.
struct SerialFmt { const char* name; uint32_t conf; };
static const SerialFmt SERIAL_FMTS[] = {
  {"8N1", SERIAL_8N1}, {"8E1", SERIAL_8E1}, {"8O1", SERIAL_8O1}, {"8N2", SERIAL_8N2},
};
static constexpr uint8_t SERIAL_FMT_COUNT = sizeof(SERIAL_FMTS) / sizeof(SERIAL_FMTS[0]);
static const uint32_t BAUDS[] = {1200, 2400, 4800, 9600, 19200, 38400, 57600, 115200};
static constexpr uint8_t BAUD_COUNT = sizeof(BAUDS) / sizeof(BAUDS[0]);

// Published by NetTask, read by loop(). Aligned 32-bit -> atomic on Xtensa.
static volatile uint32_t g_netHeartbeatMs = 0;
static volatile uint32_t g_lastTrafficMs  = 0;
static volatile bool     g_linkUp         = false;
static volatile uint32_t g_rtuHeartbeatMs = 0;          // published by RtuTask
static volatile bool     g_ethOk          = false;      // W5500 present & sane (for the LEDs)

// Everything below is touched ONLY by NetTask (or setup() before it starts).
static bool     g_twdtSubscribed = false;   // NetTask on task-WDT
static bool     g_rtuTwdtSubscribed = false; // RtuTask on task-WDT (written only by RtuTask)
static bool     g_ethReady       = false;
static bool     g_restartPending = false;
static uint32_t g_restartAtMs    = 0;
static uint8_t  g_ethStrikes     = 0;       // consecutive failed health reads

// RTU timing derived from baud rate
static uint32_t g_charTimeUs = 0;   // 11-bit character
static uint32_t g_t15Us      = 0;   // inter-character limit inside a frame
static uint32_t g_t35Us      = 0;   // inter-frame silence
static uint32_t g_txGuardUs  = 0;   // DE hold after TX-done
static uint32_t g_busFreeAtUs = 0;  // micros() when bus may be driven again

// Set by NetTask when a job is abandoned, cleared by RtuTask before the next
// transaction: the UART must be drained to silence before the bus is used
// again, because an abandoned transaction may have left bytes in flight
// (audit FIX-03). Single writer in each direction; a lost update only costs
// one extra drain.
static volatile bool g_rtuResyncNeeded = false;

// Measured slave turnaround (TX complete -> first response byte). RtuTask
// writes, NetTask reads for the status page: the manual equivalent of MOXA's
// response-timeout "Auto Detection" (audit FIX-A16).
static volatile uint32_t g_turnaroundLastUs = 0;
static volatile uint32_t g_turnaroundMaxUs  = 0;
static volatile uint32_t g_turnaroundMinUs  = 0xFFFFFFFFUL;

// Counters are 32-bit and RAM-only: they wrap after ~4.3e9 events and reset on
// every reboot. They are a live diagnostic aid, not an audit log.
struct Stats {
  uint32_t requests, rtuOk, rtuTimeout, rtuCrc, rtuFrame, rtuUnitMismatch, rtuFcMismatch;
  uint32_t broadcasts, reqRejected, reqAged, staleResults, rtuAbandoned, internalErr;
  uint32_t tcpAccepted, tcpRejected, tcpEvicted, tcpDropped, tcpDisconnects;
  uint32_t queueBackpressure, httpWriteFail, authFailures, authLockouts;
  uint32_t cfgSaveFail, cfgCrcFail, ethRecoveries;
};
static Stats g_stat = {};

// Per-unit-id health, so a single bad slave can be identified from the status
// page instead of a bus analyser (audit FIX-A11). Least-used slot is recycled.
struct UnitStat { uint8_t unit; bool used; uint32_t ok, fail; };
static UnitStat g_unitStat[UNIT_STAT_SLOTS] = {};

static void unitStatRecord(uint8_t unit, bool ok) {
  UnitStat* victim = nullptr;
  for (auto& u : g_unitStat) {
    if (u.used && u.unit == unit) {
      if (ok) u.ok++; else u.fail++;
      return;
    }
    if (!u.used) { victim = &u; break; }
    if (!victim || (u.ok + u.fail) < (victim->ok + victim->fail)) victim = &u;
  }
  if (!victim) return;
  victim->used = true; victim->unit = unit;
  victim->ok = ok ? 1 : 0;
  victim->fail = ok ? 0 : 1;
}

// Written by the UART event task, read by NetTask -> volatile, 32-bit.
static volatile uint32_t g_uartFrameErr  = 0;
static volatile uint32_t g_uartParityErr = 0;
static volatile uint32_t g_uartOverflow  = 0;

// ==========================================================================
// 4. SMALL UTILITIES
// ==========================================================================
static inline void pulseExternalWatchdog() {
  digitalWrite(WDT_DONE_PIN, HIGH);
  delayMicroseconds(50);
  digitalWrite(WDT_DONE_PIN, LOW);
}

// Called by NetTask at every point where it may wait.
static inline void netAlive() {
  g_netHeartbeatMs = millis();
  if (g_twdtSubscribed) esp_task_wdt_reset();
}

// Called by RtuTask at every point where it may wait.
static inline void rtuAlive() {
  g_rtuHeartbeatMs = millis();
  if (g_rtuTwdtSubscribed) esp_task_wdt_reset();
}

static inline uint32_t ipToU32(const IPAddress& a) {
  return ((uint32_t)a[0] << 24) | ((uint32_t)a[1] << 16) | ((uint32_t)a[2] << 8) | a[3];
}

// Strict dotted-quad parser (IPAddress::fromString on core 3.x also accepts IPv6).
static bool parseIPv4(const char* s, IPAddress& out) {
  uint8_t oct[4];
  for (int i = 0; i < 4; i++) {
    if (!isdigit((unsigned char)*s)) return false;
    uint32_t v = 0; int digits = 0;
    while (isdigit((unsigned char)*s)) {
      v = v * 10 + (uint32_t)(*s++ - '0');
      if (++digits > 3 || v > 255) return false;
    }
    oct[i] = (uint8_t)v;
    if (i < 3) { if (*s != '.') return false; s++; }
  }
  if (*s != '\0') return false;
  out = IPAddress(oct[0], oct[1], oct[2], oct[3]);
  return true;
}

static bool parseU32(const char* s, uint32_t minV, uint32_t maxV, uint32_t& out) {
  if (!isdigit((unsigned char)*s)) return false;
  char* end = nullptr;
  unsigned long v = strtoul(s, &end, 10);
  if (!end || *end != '\0' || v < minV || v > maxV) return false;
  out = (uint32_t)v;
  return true;
}

static bool maskValid(const IPAddress& m) {
  uint32_t v = ipToU32(m);
  uint32_t inv = ~v;
  return v != 0 && (inv & (inv + 1)) == 0 && inv >= 3;   // contiguous, /1 .. /30
}

static bool networkValid(const IPAddress& ip, const IPAddress& sn, const IPAddress& gw) {
  if (!maskValid(sn)) return false;
  uint32_t i = ipToU32(ip), m = ipToU32(sn), g = ipToU32(gw);
  if (ip[0] == 0 || ip[0] == 127 || ip[0] >= 224) return false;
  if ((i & ~m) == 0 || (i & ~m) == ~m) return false;             // network / broadcast addr
  if (g != 0 && ((g & m) != (i & m) || g == i || (g & ~m) == ~m)) return false;
  return true;
}

static bool baudValid(uint32_t b) {
  for (uint8_t i = 0; i < BAUD_COUNT; i++) if (BAUDS[i] == b) return true;
  return false;
}

static bool portValid(uint32_t p) { return p >= 1 && p <= 65535 && p != HTTP_PORT; }

// --------------------------------------------------------------------------
// Minimal SHA-256 (FIPS 180-4). Self-contained on purpose: the mbedTLS SHA
// entry points were renamed between IDF 4.x and 5.x, and the web password hash
// must not depend on which Arduino core the firmware is built with.
// --------------------------------------------------------------------------
typedef struct { uint32_t state[8]; uint64_t bits; uint8_t buf[64]; size_t idx; } Sha256Ctx;

static const uint32_t SHA256_K[64] = {
  0x428a2f98UL,0x71374491UL,0xb5c0fbcfUL,0xe9b5dba5UL,0x3956c25bUL,0x59f111f1UL,0x923f82a4UL,0xab1c5ed5UL,
  0xd807aa98UL,0x12835b01UL,0x243185beUL,0x550c7dc3UL,0x72be5d74UL,0x80deb1feUL,0x9bdc06a7UL,0xc19bf174UL,
  0xe49b69c1UL,0xefbe4786UL,0x0fc19dc6UL,0x240ca1ccUL,0x2de92c6fUL,0x4a7484aaUL,0x5cb0a9dcUL,0x76f988daUL,
  0x983e5152UL,0xa831c66dUL,0xb00327c8UL,0xbf597fc7UL,0xc6e00bf3UL,0xd5a79147UL,0x06ca6351UL,0x14292967UL,
  0x27b70a85UL,0x2e1b2138UL,0x4d2c6dfcUL,0x53380d13UL,0x650a7354UL,0x766a0abbUL,0x81c2c92eUL,0x92722c85UL,
  0xa2bfe8a1UL,0xa81a664bUL,0xc24b8b70UL,0xc76c51a3UL,0xd192e819UL,0xd6990624UL,0xf40e3585UL,0x106aa070UL,
  0x19a4c116UL,0x1e376c08UL,0x2748774cUL,0x34b0bcb5UL,0x391c0cb3UL,0x4ed8aa4aUL,0x5b9cca4fUL,0x682e6ff3UL,
  0x748f82eeUL,0x78a5636fUL,0x84c87814UL,0x8cc70208UL,0x90befffaUL,0xa4506cebUL,0xbef9a3f7UL,0xc67178f2UL };

static inline uint32_t shaRor(uint32_t x, uint32_t n) { return (x >> n) | (x << (32 - n)); }

static void sha256Block(Sha256Ctx* c, const uint8_t* p) {
  uint32_t w[64], a, b, cc, d, e, f, g, h;
  for (int i = 0; i < 16; i++)
    w[i] = ((uint32_t)p[i*4] << 24) | ((uint32_t)p[i*4+1] << 16) | ((uint32_t)p[i*4+2] << 8) | p[i*4+3];
  for (int i = 16; i < 64; i++) {
    uint32_t s0 = shaRor(w[i-15],7) ^ shaRor(w[i-15],18) ^ (w[i-15] >> 3);
    uint32_t s1 = shaRor(w[i-2],17) ^ shaRor(w[i-2],19) ^ (w[i-2] >> 10);
    w[i] = w[i-16] + s0 + w[i-7] + s1;
  }
  a=c->state[0]; b=c->state[1]; cc=c->state[2]; d=c->state[3];
  e=c->state[4]; f=c->state[5]; g=c->state[6];  h=c->state[7];
  for (int i = 0; i < 64; i++) {
    uint32_t S1 = shaRor(e,6) ^ shaRor(e,11) ^ shaRor(e,25);
    uint32_t ch = (e & f) ^ ((~e) & g);
    uint32_t t1 = h + S1 + ch + SHA256_K[i] + w[i];
    uint32_t S0 = shaRor(a,2) ^ shaRor(a,13) ^ shaRor(a,22);
    uint32_t mj = (a & b) ^ (a & cc) ^ (b & cc);
    uint32_t t2 = S0 + mj;
    h=g; g=f; f=e; e=d+t1; d=cc; cc=b; b=a; a=t1+t2;
  }
  c->state[0]+=a; c->state[1]+=b; c->state[2]+=cc; c->state[3]+=d;
  c->state[4]+=e; c->state[5]+=f; c->state[6]+=g; c->state[7]+=h;
}

static void sha256(const uint8_t* data, size_t len, uint8_t out[32]) {
  Sha256Ctx c;
  c.state[0]=0x6a09e667UL; c.state[1]=0xbb67ae85UL; c.state[2]=0x3c6ef372UL; c.state[3]=0xa54ff53aUL;
  c.state[4]=0x510e527fUL; c.state[5]=0x9b05688cUL; c.state[6]=0x1f83d9abUL; c.state[7]=0x5be0cd19UL;
  c.bits = (uint64_t)len * 8; c.idx = 0;
  size_t i = 0;
  while (len - i >= 64) { sha256Block(&c, data + i); i += 64; }
  size_t rem = len - i;
  memcpy(c.buf, data + i, rem);
  c.buf[rem++] = 0x80;
  if (rem > 56) { memset(c.buf + rem, 0, 64 - rem); sha256Block(&c, c.buf); rem = 0; }
  memset(c.buf + rem, 0, 56 - rem);
  for (int k = 0; k < 8; k++) c.buf[56 + k] = (uint8_t)(c.bits >> (56 - 8 * k));
  sha256Block(&c, c.buf);
  for (int k = 0; k < 8; k++) {
    out[k*4]   = (uint8_t)(c.state[k] >> 24); out[k*4+1] = (uint8_t)(c.state[k] >> 16);
    out[k*4+2] = (uint8_t)(c.state[k] >> 8);  out[k*4+3] = (uint8_t)(c.state[k]);
  }
}

// Constant-time buffer compare (no early exit on the first differing byte).
static bool ctEqualBytes(const uint8_t* a, const uint8_t* b, size_t n) {
  uint8_t d = 0;
  for (size_t i = 0; i < n; i++) d |= (uint8_t)(a[i] ^ b[i]);
  return d == 0;
}

// "user:password" -> sha256. The plaintext exists only for the microseconds it
// takes to hash a login attempt; only the hash is ever stored.
static void hashCredentials(const char* user, const char* pass, uint8_t out[32]) {
  char joined[WEB_PASS_MAX_LEN + 40];
  snprintf(joined, sizeof(joined), "%s:%s", user, pass);
  sha256((const uint8_t*)joined, strlen(joined), out);
  memset(joined, 0, sizeof(joined));
}

// Every device gets its own default password, derived from its MAC, so that a
// leaked firmware image does not unlock the installed base. The value is NOT
// logged (audit FIX-A5): it is printed on the device label.
static void defaultPassword(char* out, size_t cap) {
  if (sizeof(WEB_DEFAULT_PASSWORD) > 1) {            // fixed, known password
    snprintf(out, cap, "%s", WEB_DEFAULT_PASSWORD);
    return;
  }
  // Per-device fallback. Never callable before generateUniqueMac(): an all-zero
  // MAC would produce the same password on every device and would not match the
  // label.
  if ((mac[0] | mac[1] | mac[2] | mac[3] | mac[4] | mac[5]) == 0)
    LOGF("[SEC] BUG: default password requested before the MAC was generated\n");
  snprintf(out, cap, "MG-%02X%02X%02X", mac[3], mac[4], mac[5]);
}

// ==========================================================================
// 5. CONFIG (NVS) — one atomic versioned blob (legacy keys still readable)
// ==========================================================================
// One write, one CRC, one version: a power cut during save can no longer leave
// a half-new configuration behind (audit FIX-04). Legacy per-key values from
// firmware <= 2.1.0 are read once and migrated into the blob.
// One record holds EVERYTHING that must survive a reboot together: network,
// serial, gateway options, the password hash and the default-password flag.
// It is written to two NVS slots in turn, each with its own generation number
// and CRC, and read back to verify. Power loss at any instant therefore leaves
// at least one fully valid slot, and the configuration can never disagree with
// the password (audit FIX-A2 / FIX-A6).
#define CFG_MAGIC   0xA71Cu
#define CFG_VERSION 3u        // v1/v2 single-key blobs are migrated on first boot

struct StoredCfg {
  uint16_t magic;
  uint16_t version;
  uint32_t generation;          // higher = newer; the valid slot with the highest wins
  uint8_t  ip[4], sn[4], gw[4];
  uint16_t port;
  uint32_t baud;
  uint8_t  fmt;
  uint8_t  flags;
  uint16_t rtuTimeoutMs;
  uint8_t  pwHash[32];          // sha256("user:password")
  uint8_t  pwIsDefault;
  uint8_t  reserved[7];         // room for future fields without a new version
  uint16_t crc;                 // modbusCRC over every byte before this field
} __attribute__((packed));

// Legacy layouts, read once so that an upgrade keeps the operator's settings.
struct StoredCfgV2 {
  uint16_t magic; uint16_t version;
  uint8_t  ip[4], sn[4], gw[4];
  uint16_t port; uint32_t baud;
  uint8_t  fmt; uint8_t flags; uint16_t rtuTimeoutMs;
  uint16_t crc;
} __attribute__((packed));

static uint16_t modbusCRC(const uint8_t* buf, size_t len);   // defined in section 9

static const char* CFG_SLOT[2] = { "cfgA", "cfgB" };
static uint32_t g_cfgGeneration = 0;
static uint8_t  g_cfgSlot = 0;          // slot the live configuration came from

static uint16_t cfgCrc(const StoredCfg& b) {
  return modbusCRC((const uint8_t*)&b, sizeof(StoredCfg) - sizeof(uint16_t));
}

static bool configBlobValid(const StoredCfg& b) {
  return b.magic == CFG_MAGIC && b.version == CFG_VERSION && b.crc == cfgCrc(b);
}

static void applyDefaults(GatewayConfig& c) {
  c.ip = IPAddress(192, 168, 1, 50);
  c.sn = IPAddress(255, 255, 255, 0);
  c.gw = IPAddress(192, 168, 1, 1);
  c.port = 502; c.baud = 9600; c.fmt = 0;
  c.flags = CFG_FLAG_TCP_EXCEPTION;              // MOXA's "Modbus TCP Exception" = enabled
  c.rtuTimeoutMs = RS485_DEFAULT_TIMEOUT_MS;
}

static bool configSane(const GatewayConfig& c) {
  return networkValid(c.ip, c.sn, c.gw) && portValid(c.port) && baudValid(c.baud) &&
         c.fmt < SERIAL_FMT_COUNT && c.rtuTimeoutMs >= 20 && c.rtuTimeoutMs <= 5000;
}

// Runtime credential state (mirrors the blob).
static uint8_t g_pwHash[32];
static bool    g_pwIsDefault = false;

// Write the complete record to the slot that is NOT in use, then read it back
// and verify. Only after that does the caller's RAM state become authoritative.
static bool persistConfig(const GatewayConfig& c, const uint8_t pwHash[32], bool pwDefault) {
  StoredCfg b;
  memset(&b, 0, sizeof(b));
  b.magic = CFG_MAGIC;
  b.version = CFG_VERSION;
  b.generation = g_cfgGeneration + 1;
  for (int i = 0; i < 4; i++) { b.ip[i] = c.ip[i]; b.sn[i] = c.sn[i]; b.gw[i] = c.gw[i]; }
  b.port = c.port; b.baud = c.baud; b.fmt = c.fmt; b.flags = c.flags;
  b.rtuTimeoutMs = c.rtuTimeoutMs;
  memcpy(b.pwHash, pwHash, sizeof(b.pwHash));
  b.pwIsDefault = pwDefault ? 1 : 0;
  b.crc = cfgCrc(b);

  const uint8_t target = g_cfgSlot ^ 1;            // never overwrite the live slot
  if (prefs.putBytes(CFG_SLOT[target], &b, sizeof(b)) != sizeof(b)) {
    LOGF("[CFG] write to %s failed\n", CFG_SLOT[target]);
    return false;
  }
  StoredCfg check;
  if (prefs.getBytes(CFG_SLOT[target], &check, sizeof(check)) != sizeof(check) ||
      !configBlobValid(check) || check.generation != b.generation ||
      memcmp(&check, &b, sizeof(b)) != 0) {
    LOGF("[CFG] read-back verify of %s failed\n", CFG_SLOT[target]);
    return false;
  }
  g_cfgSlot = target;                              // commit point
  g_cfgGeneration = b.generation;
  return true;
}

// Legacy migration: v1/v2 blob under "cfg" plus "pwh"/"pwdef".
static bool loadLegacyBlob(GatewayConfig& c, uint8_t pwHash[32], bool& pwDefault) {
  StoredCfgV2 b;
  if (prefs.getBytes("cfg", &b, sizeof(b)) != sizeof(b)) return false;
  if (b.magic != CFG_MAGIC || (b.version != 1u && b.version != 2u)) return false;
  if (b.crc != modbusCRC((const uint8_t*)&b, sizeof(StoredCfgV2) - sizeof(uint16_t))) return false;
  c.ip = IPAddress(b.ip[0], b.ip[1], b.ip[2], b.ip[3]);
  c.sn = IPAddress(b.sn[0], b.sn[1], b.sn[2], b.sn[3]);
  c.gw = IPAddress(b.gw[0], b.gw[1], b.gw[2], b.gw[3]);
  c.port = b.port; c.baud = b.baud; c.fmt = b.fmt;
  c.flags = (b.version >= 2u) ? b.flags : CFG_FLAG_TCP_EXCEPTION;
  c.rtuTimeoutMs = b.rtuTimeoutMs;
  if (prefs.getBytes("pwh", pwHash, 32) == 32) pwDefault = prefs.getBool("pwdef", false);
  else                                          return false;
  return configSane(c);
}

// Even older layout: one NVS key per value.
static bool loadLegacyKeys(GatewayConfig& c) {
  char buf[20];
  IPAddress ip, sn, gw;
  if (prefs.getString("l_ip", buf, sizeof(buf)) == 0 || !parseIPv4(buf, ip)) return false;
  if (prefs.getString("l_sn", buf, sizeof(buf)) == 0 || !parseIPv4(buf, sn)) return false;
  if (prefs.getString("l_gw", buf, sizeof(buf)) == 0 || !parseIPv4(buf, gw)) return false;
  c.ip = ip; c.sn = sn; c.gw = gw;
  c.port = (uint16_t)prefs.getInt("m_port", 502);
  c.baud = (uint32_t)prefs.getInt("baud", 9600);
  c.fmt  = prefs.getUChar("fmt", 0);
  c.flags = CFG_FLAG_TCP_EXCEPTION;
  c.rtuTimeoutMs = prefs.getUShort("rtu_to", RS485_DEFAULT_TIMEOUT_MS);
  return configSane(c);
}

static void dropLegacyKeys() {
  const char* keys[] = {"cfg", "pwh", "pwdef", "l_ip", "l_sn", "l_gw", "m_port", "baud", "fmt", "rtu_to"};
  for (const char* k : keys) prefs.remove(k);
}

// Boot: validate both slots, take the newest valid generation, migrate anything
// older, and fall back to factory defaults only if nothing is usable.
static void loadConfig() {
  applyDefaults(cfg);

  StoredCfg slot[2];
  bool ok[2] = {false, false};
  for (int i = 0; i < 2; i++) {
    // isKey() first: reading a missing blob makes the Preferences library log an
    // alarming "nvs_get_blob len fail" error on a perfectly normal first boot.
    ok[i] = prefs.isKey(CFG_SLOT[i]) &&
            prefs.getBytes(CFG_SLOT[i], &slot[i], sizeof(StoredCfg)) == sizeof(StoredCfg) &&
            configBlobValid(slot[i]);
  }

  if (!ok[0] && !ok[1] &&
      (prefs.isKey(CFG_SLOT[0]) || prefs.isKey(CFG_SLOT[1]))) g_stat.cfgCrcFail++;

  int chosen = -1;
  if (ok[0] && ok[1]) chosen = (int32_t)(slot[0].generation - slot[1].generation) >= 0 ? 0 : 1;
  else if (ok[0])     chosen = 0;
  else if (ok[1])     chosen = 1;

  if (chosen >= 0) {
    const StoredCfg& b = slot[chosen];
    GatewayConfig tmp;
    tmp.ip = IPAddress(b.ip[0], b.ip[1], b.ip[2], b.ip[3]);
    tmp.sn = IPAddress(b.sn[0], b.sn[1], b.sn[2], b.sn[3]);
    tmp.gw = IPAddress(b.gw[0], b.gw[1], b.gw[2], b.gw[3]);
    tmp.port = b.port; tmp.baud = b.baud; tmp.fmt = b.fmt; tmp.flags = b.flags;
    tmp.rtuTimeoutMs = b.rtuTimeoutMs;
    if (configSane(tmp)) {
      cfg = tmp;
      memcpy(g_pwHash, b.pwHash, sizeof(g_pwHash));
      g_pwIsDefault = b.pwIsDefault != 0;
      g_cfgSlot = (uint8_t)chosen;
      g_cfgGeneration = b.generation;
      LOGF("[CFG] loaded slot %s generation %lu\n", CFG_SLOT[chosen], (unsigned long)b.generation);

      // Commissioning mode: make sure the known default password is the one in
      // force, whatever is stored. Also the self-heal for units flashed with the
      // broken initialisation order, where the stored hash was computed from an
      // all-zero MAC (audit FIX-01 / FIX-61). Flash is written only when the
      // hash actually differs, so a normal boot writes nothing.
      if (g_pwIsDefault || WEB_FORCE_DEFAULT_PASSWORD) {
        char def[16];
        uint8_t expect[32];
        defaultPassword(def, sizeof(def));
        hashCredentials(WEB_AUTH_USER, def, expect);
        if (!ctEqualBytes(expect, g_pwHash, sizeof(expect)) || !g_pwIsDefault) {
          memcpy(g_pwHash, expect, sizeof(g_pwHash));
          g_pwIsDefault = true;
          if (persistConfig(cfg, g_pwHash, true))
            LOGF("[SEC] web password reset to the built-in default\n");
          else
            g_stat.cfgSaveFail++;
        }
      }
      return;
    }
    LOGF("[CFG] stored configuration failed validation\n");
    g_stat.cfgCrcFail++;
  }

  // Migration paths (run once, then the A/B slots take over).
  GatewayConfig legacy;
  applyDefaults(legacy);
  uint8_t pwHash[32];
  bool pwDefault = true;
  bool haveLegacy = loadLegacyBlob(legacy, pwHash, pwDefault);
  if (!haveLegacy && loadLegacyKeys(legacy)) {
    char def[16];
    defaultPassword(def, sizeof(def));
    hashCredentials(WEB_AUTH_USER, def, pwHash);
    pwDefault = true;
    haveLegacy = true;
  }
  if (haveLegacy) {
    cfg = legacy;
    memcpy(g_pwHash, pwHash, sizeof(g_pwHash));
    g_pwIsDefault = pwDefault;
    if (persistConfig(cfg, g_pwHash, g_pwIsDefault)) {
      dropLegacyKeys();
      LOGF("[CFG] migrated older configuration into the A/B slots\n");
    }
    return;
  }

  // Nothing usable: factory defaults with the per-device default password.
  char def[16];
  defaultPassword(def, sizeof(def));
  hashCredentials(WEB_AUTH_USER, def, g_pwHash);
  g_pwIsDefault = true;
  if (persistConfig(cfg, g_pwHash, g_pwIsDefault))
    LOGF("[CFG] no valid configuration found -> factory defaults stored\n");
}

static void computeRtuTiming() {
  g_charTimeUs = (11UL * 1000000UL + cfg.baud - 1) / cfg.baud;
  g_t15Us      = (cfg.baud > 19200) ? 750  : (g_charTimeUs * 3 + 1) / 2;  // 1.5 chars
  g_t35Us      = (cfg.baud > 19200) ? 1750 : (g_charTimeUs * 7 + 1) / 2;  // 3.5 chars
  g_txGuardUs  = (2UL * 1000000UL + cfg.baud - 1) / cfg.baud + 5;          // ~2 bit times
}

static void factoryResetCheck() {
#if FACTORY_RESET_PIN >= 0
  pinMode(FACTORY_RESET_PIN, INPUT_PULLUP);
  delay(10);
  if (digitalRead(FACTORY_RESET_PIN) != LOW) return;
  LOGF("[CFG] factory-reset pin held...\n");
  uint32_t start = millis();
  while (digitalRead(FACTORY_RESET_PIN) == LOW) {
    if (millis() - start >= FACTORY_RESET_HOLD_MS) {
      prefs.clear();
      LOGF("[CFG] configuration wiped (factory defaults)\n");
      for (int i = 0; i < 10; i++) { digitalWrite(LED_D3, i & 1); digitalWrite(LED_D4, i & 1); delay(100); }
      return;
    }
    pulseExternalWatchdog();
    delay(50);
  }
#endif
}

// ==========================================================================
// 6. UNIQUE MAC
// ==========================================================================
static void generateUniqueMac(byte* macOut) {
  uint8_t base[6];
  esp_read_mac(base, ESP_MAC_WIFI_STA);
  memcpy(macOut, base, 6);
  macOut[0] = (uint8_t)((macOut[0] | 0x02) & 0xFE);  // locally administered + unicast
  macOut[5] ^= 0x01;
}

// ==========================================================================
// 7. SOCKET OWNERSHIP HELPERS  (NetTask only)
// ==========================================================================
// Zero-initialised "no socket" client (static storage). Assigning from this
// instead of a temporary avoids core 3.x -Wuninitialized noise in Stream.
static EthernetClient kNoClient;

// One parsed, validated Modbus request waiting for the bus. Every field the
// response needs is copied here, so the raw TCP buffer can move on and the
// client's identity can never be inferred from shared state (audit FIX-A1).
struct PendingReq {
  uint8_t  tidHi, tidLo;        // transaction id: scoped to THIS connection
  uint8_t  unit, fc;
  uint16_t rtuLen;              // unit + pdu, no CRC
  uint32_t enqueuedMs;
  uint8_t  rtu[MB_RTU_MAX];
};

struct MbSession {
  EthernetClient sock;
  bool     active;
  uint16_t len;                 // raw bytes held for reassembly
  uint32_t lastRxMs;
  uint32_t frameStartMs;
  bool     busy;                // one request of this client is on the bus
  uint32_t gen;                 // bumped on every close/reuse -> stale results are dropped
  uint8_t  qHead, qCount;       // pipelined requests, FIFO -> answers keep their order
  PendingReq q[REQ_QUEUE_DEPTH];
  uint8_t  buf[MB_TCP_ADU_MAX];
};
static MbSession g_mb[MAX_CLIENTS];

// Reset a slot's bookkeeping (socket handled by the caller).
static inline void resetMbSlot(MbSession& s) {
  s.active = false; s.len = 0; s.busy = false;
  s.qHead = 0; s.qCount = 0;
  s.gen++;
}

struct HttpSession {
  EthernetClient sock;
  bool     active;
  uint16_t len;
  uint32_t startMs;
  char     buf[HTTP_BUF_SIZE];
};
static HttpSession g_http;

// Release a socket we hold. If the chip already recycled that socket for a
// listener / an incoming handshake, it is no longer ours: just forget it.
static void releaseSocket(EthernetClient& c) {
  if (c) {
    uint8_t st = c.status();
    if (st != SockSt::CLOSED && st != SockSt::LISTEN && st != SockSt::INIT &&
        st != SockSt::SYNRECV && st != SockSt::SYNSENT) {
      c.stop();
    }
  }
  c = kNoClient;
}

static void closeMbSession(MbSession& s) {
  releaseSocket(s.sock);
  resetMbSlot(s);
}

static void closeHttpSession() {
  releaseSocket(g_http.sock);
  g_http.active = false;
  g_http.len = 0;
}

// A server just handed us socket `sn`: any older slot still pointing at the
// same socket number is stale (its connection died and the socket got
// recycled). Forget it WITHOUT stop() — stop() would kill the new client.
static void forgetStaleHolders(uint8_t sn) {
  for (auto& s : g_mb) {
    if (s.active && s.sock.getSocketNumber() == sn) {
      s.sock = kNoClient; resetMbSlot(s);
    }
  }
  if (g_http.active && g_http.sock.getSocketNumber() == sn) {
    g_http.sock = kNoClient; g_http.active = false; g_http.len = 0;
  }
}

// Forget every socket without touching the chip (used after a W5500 reset).
static void forgetAllSockets() {
  for (auto& s : g_mb) { s.sock = kNoClient; resetMbSlot(s); }
  g_http.sock = kNoClient; g_http.active = false; g_http.len = 0;
}

static bool socketWritable(EthernetClient& c) {
  uint8_t st = c.status();
  return st == SockSt::ESTABLISHED || st == SockSt::CLOSE_WAIT;
}

// Chunked, bounded write. EthernetClient::write() truncates silently at the
// W5500 socket buffer size and busy-waits on a stalled peer, so never hand
// it more than it can take right now.
// Absolute deadline for the WHOLE transfer (audit FIX-07): a peer that accepts
// one byte at a time must not be able to hold NetTask indefinitely.
static bool writeAll(EthernetClient& c, const uint8_t* data, size_t len,
                     uint32_t timeoutMs = TCP_WRITE_TIMEOUT_MS) {
  const uint32_t deadline = millis() + timeoutMs;
  while (len > 0) {
    if (!socketWritable(c)) return false;
    if ((int32_t)(millis() - deadline) > 0) return false;
    int room = c.availableForWrite();
    if (room <= 0) {
      netAlive();
      vTaskDelay(1);
      continue;
    }
    size_t chunk = len;
    if (chunk > (size_t)room) chunk = (size_t)room;
    if (chunk > 1024) chunk = 1024;
    if (c.write(data, chunk) != chunk) return false;
    data += chunk; len -= chunk;
  }
  return true;
}

// ==========================================================================
// 8. W5500 INIT / HEALTH  (setup() at boot, NetTask afterwards)
// ==========================================================================
// --------------------------------------------------------------------------
// Raw W5500 probe. The Arduino Ethernet library detects the chip at a fixed
// 14 MHz; this talks to the same chip at a chosen clock using the W5500's own
// variable-length data mode, so a wiring/level/clock problem can be told apart
// from a library problem (audit FIX-51).
//   frame: [addr hi][addr lo][control: BSB=0, read, VDM] then read one byte
// --------------------------------------------------------------------------
static uint8_t w5500RawVersion(uint32_t clockHz) {
  SPI.beginTransaction(SPISettings(clockHz, MSBFIRST, SPI_MODE0));
  digitalWrite(W5500_CS, LOW);
  SPI.transfer((uint8_t)(W5500_VERSIONR >> 8));
  SPI.transfer((uint8_t)(W5500_VERSIONR & 0xFF));
  SPI.transfer(0x00);                       // common register block, read, VDM
  uint8_t v = SPI.transfer(0x00);
  digitalWrite(W5500_CS, HIGH);
  SPI.endTransaction();
  return v;
}

// Print which SPI clocks the chip answers at. Only used when something is
// wrong, so the cost does not matter (audit FIX-52).
static void w5500ClockSweep() {
  static const uint32_t clocks[] = {1000000UL, 2000000UL, 4000000UL,
                                    8000000UL, 14000000UL, 20000000UL};
  LOGF("[ETH] SPI clock sweep (VERSIONR must read 0x%02X):\n", W5500_EXPECTED_VERSION);
  for (uint32_t hz : clocks) {
    const uint8_t v = w5500RawVersion(hz);
    (void)v;                                  // only used by the log line
    LOGF("[ETH]   %2lu MHz -> 0x%02X %s\n", (unsigned long)(hz / 1000000UL), v,
         v == W5500_EXPECTED_VERSION ? "OK" : "no answer");
  }
  LOGF("[ETH] If NOTHING answers at any clock, the most likely cause is the RESET\n"
       "[ETH] line: this build asserts reset with GPIO %s. If your board drives\n"
       "[ETH] /RESET directly instead of through a transistor, rebuild with\n"
       "[ETH] -DW5500_RST_ASSERT_HIGH=%d.\n",
       W5500_RST_ASSERT_HIGH ? "HIGH" : "LOW", W5500_RST_ASSERT_HIGH ? 0 : 1);
  LOGF("[ETH] If low clocks answer but 14 MHz does not, the wiring cannot carry\n"
       "[ETH] the library's fixed 14 MHz: shorten the SPI wires, add ground\n"
       "[ETH] return paths, or lower SPI_ETHERNET_SETTINGS in Ethernet/src/\n"
       "[ETH] utility/w5100.h (line ~21) to SPISettings(8000000, MSBFIRST, SPI_MODE0).\n"
       "[ETH] If NO clock answers: check 3V3 supply and current, MISO/MOSI not\n"
       "[ETH] swapped, CS wired to the configured pin, and RESET not held low.\n");
}

// Assert reset, release it, then WAIT FOR THE CHIP to answer instead of hoping
// a fixed delay is enough. The measured boot time is a crystal health indicator:
// a healthy W5500 answers in well under 250 ms (audit FIX-57).
static bool w5500HardwareReset(uint32_t* bootMsOut) {
  if (bootMsOut) *bootMsOut = 0;
#if W5500_RST >= 0
  digitalWrite(W5500_RST, W5500_RST_ASSERT_LEVEL);
  delay(W5500_RST_PULSE_MS);
  digitalWrite(W5500_RST, W5500_RST_RELEASE_LEVEL);
#endif
  delay(100);                                 // let the PLL start before polling

  const uint32_t start = millis();
  while (millis() - start < W5500_BOOT_TIMEOUT_MS) {
    if (w5500RawVersion(ETH_PROBE_HZ) == W5500_EXPECTED_VERSION) {
      if (bootMsOut) *bootMsOut = millis() - start;
      return true;
    }
    pulseExternalWatchdog();
    delay(5);
  }
  return false;
}

static bool ethernetInit() {
  pinMode(W5500_CS, OUTPUT);
  digitalWrite(W5500_CS, HIGH);               // deselect before any transfer

  for (uint8_t attempt = 1; attempt <= ETH_INIT_ATTEMPTS; attempt++) {
    uint32_t bootMs = 0;
    const bool responding = w5500HardwareReset(&bootMs);
    pulseExternalWatchdog();

    if (responding) {
      LOGF("[ETH] attempt %u/%u: chip booted in %lu ms, VERSIONR = 0x%02X%s\n",
           attempt, (unsigned)ETH_INIT_ATTEMPTS, (unsigned long)bootMs,
           W5500_EXPECTED_VERSION,
           bootMs > 250 ? "  (slow boot - check the 25 MHz crystal and its caps)" : "");
    } else {
      LOGF("[ETH] attempt %u/%u: no answer within %u ms (probe read 0x%02X at %lu MHz)\n",
           attempt, (unsigned)ETH_INIT_ATTEMPTS, (unsigned)W5500_BOOT_TIMEOUT_MS,
           w5500RawVersion(ETH_PROBE_HZ), (unsigned long)(ETH_PROBE_HZ / 1000000UL));
    }

    Ethernet.init(W5500_CS);
    Ethernet.begin(mac, cfg.ip, cfg.gw, cfg.gw, cfg.sn);   // mac, ip, dns, gw, subnet

    if (Ethernet.hardwareStatus() != EthernetNoHardware) {
      Ethernet.setRetransmissionTimeout(W5500_RETX_TIMEOUT_MS);
      Ethernet.setRetransmissionCount(W5500_RETX_COUNT);
      modbusServer.begin();
      httpServer.begin();
      LOGF("[ETH] W5500 ready (link %s)\n",
           Ethernet.linkStatus() == LinkON ? "UP" : "down");
      return true;
    }
    LOGF("[ETH] attempt %u/%u: library did not detect the chip\n",
         attempt, (unsigned)ETH_INIT_ATTEMPTS);
    pulseExternalWatchdog();
  }

  // Out of attempts: say exactly what was seen and what to check.
  LOGF("[ETH] W5500 initialisation FAILED after %u attempts\n", (unsigned)ETH_INIT_ATTEMPTS);
  w5500ClockSweep();
  LOGF("[ETH] pins in use: SCLK %d  MISO %d  MOSI %d  CS %d  RST %d\n",
       W5500_SCLK, W5500_MISO, W5500_MOSI, W5500_CS, (int)W5500_RST);
  return false;
}

// hardwareStatus() only reports the chip id cached during init(), so it proves
// nothing about the chip right now. VERSIONR is a live read over SPI, and
// SIPR/SUBR prove the configuration the library wrote is still in the chip.
// A reset by EMI or a dead SPI bus fails all three (audit FIX-55).
static bool ethernetHealthy() {
  const uint8_t ver = w5500RawVersion(ETH_PROBE_HZ);
  if (ver != W5500_EXPECTED_VERSION) {
    LOGF("[ETH] health: VERSIONR 0x%02X (expected 0x%02X)\n", ver, W5500_EXPECTED_VERSION);
    return false;
  }
  IPAddress ip = Ethernet.localIP(), sn = Ethernet.subnetMask();
  if (ip == cfg.ip && sn == cfg.sn) return true;
  LOGF("[ETH] health: chip holds %u.%u.%u.%u/%u.%u.%u.%u, expected %u.%u.%u.%u/%u.%u.%u.%u\n",
       ip[0], ip[1], ip[2], ip[3], sn[0], sn[1], sn[2], sn[3],
       cfg.ip[0], cfg.ip[1], cfg.ip[2], cfg.ip[3], cfg.sn[0], cfg.sn[1], cfg.sn[2], cfg.sn[3]);
  return false;
}

static uint32_t g_lastRecoveryMs = 0;

static void ethernetRecover() {
  // Never thrash the chip: a reinit drops every TCP session, so it happens at
  // most once per ETH_RECOVERY_MIN_INTERVAL_MS (audit FIX-55).
  if (g_lastRecoveryMs && millis() - g_lastRecoveryMs < ETH_RECOVERY_MIN_INTERVAL_MS) return;
  g_lastRecoveryMs = millis();
  // Note: the chip reset wipes the socket state, so connected clients are
  // dropped without a FIN. Their own TCP timeout makes them reconnect; this is
  // counted so that the event can be correlated with SCADA-side alarms.
  LOGF("[ETH] W5500 unhealthy -> re-initialising\n");
  forgetAllSockets();
  netAlive();
  g_ethReady = ethernetInit();
  g_stat.ethRecoveries++;
  netAlive();
}

// ==========================================================================
// 9. RS-485 / MODBUS RTU
// ==========================================================================
static uint16_t modbusCRC(const uint8_t* buf, size_t len) {
  uint16_t crc = 0xFFFF;
  for (size_t pos = 0; pos < len; pos++) {
    crc ^= buf[pos];
    for (int i = 0; i < 8; i++) crc = (crc & 1) ? (uint16_t)((crc >> 1) ^ 0xA001) : (uint16_t)(crc >> 1);
  }
  return crc;
}

// UART error events (wrong baud, wrong parity, noise, overrun) are counted so
// that a bad line can be told apart from a dead slave (audit FIX-10).
static void rs485ErrorCb(hardwareSerial_error_t err) {
  switch (err) {
    // Explicit read-modify-write: "volatile++" is deprecated in C++20 and the
    // UART event task is the only writer, so this is race-free as written.
    case UART_FRAME_ERROR:       g_uartFrameErr  = g_uartFrameErr  + 1; break;
    case UART_PARITY_ERROR:      g_uartParityErr = g_uartParityErr + 1; break;
    case UART_BUFFER_FULL_ERROR:
    case UART_FIFO_OVF_ERROR:    g_uartOverflow  = g_uartOverflow  + 1; break;
    default: break;
  }
}

static void rs485Init() {
  if (!rs485.setRxBufferSize(RS485_RX_BUFFER)) LOGF("[RTU] RX buffer alloc failed\n");
  rs485.begin(cfg.baud, SERIAL_FMTS[cfg.fmt].conf, RXD2, TXD2);
#if RS485_USE_HW_DE
  rs485.setPins(RXD2, TXD2, -1, RS485_DE_PIN);     // RTS = DE
  if (!rs485.setMode(UART_MODE_RS485_HALF_DUPLEX)) LOGF("[RTU] HW RS485 mode failed\n");
#endif
  // Deliver bytes to the ring buffer promptly. The IDF default (every 120
  // bytes, or after 10 idle symbols) breaks silence-based framing. One
  // interrupt per byte is affordable at low baud; above 38400 a small batch
  // keeps the interrupt load down while setRxTimeout still delimits frames.
  uint8_t fifoThreshold = (cfg.baud <= 38400) ? 1 : 8;
  if (!rs485.setRxFIFOFull(fifoThreshold)) LOGF("[RTU] setRxFIFOFull failed\n");
  if (!rs485.setRxTimeout(2))              LOGF("[RTU] setRxTimeout failed\n");
  rs485.onReceiveError(rs485ErrorCb);
}

// Read and discard everything on the line until it has been silent for t3.5
// (bounded). Used after a failed transaction so that a late reply cannot
// become the first bytes of the next one (audit FIX-03).
static void drainUntilSilent(uint32_t maxMs) {
  uint32_t start = millis();
  uint32_t lastByte = start;
  while (millis() - start < maxMs) {
    if (rs485.available() > 0) {
      while (rs485.available() > 0) rs485.read();
      lastByte = millis();
      continue;
    }
    if ((millis() - lastByte) * 1000UL > g_t35Us) return;
    rtuAlive();
    vTaskDelay(1);
  }
}

static inline void rs485TxBegin() {
#if (RS485_DE_PIN >= 0) && !RS485_USE_HW_DE
  digitalWrite(RS485_DE_PIN, HIGH);
  delayMicroseconds(RS485_DE_SETUP_US);
#endif
}

// Returns only after the last STOP bit has physically left the UART.
// HardwareSerial::flush() alone is not enough on every core version (it may
// return when the FIFO is empty while the final byte is still shifting out).
static void rs485TxEnd(size_t frameLen) {
  uint32_t budgetMs = (uint32_t)((frameLen * g_charTimeUs) / 1000UL) + 20;
  rs485.flush();
  // Sliced wait: at 1200 baud a full frame takes >2 s, and the task watchdog
  // must keep seeing this task alive (audit FIX-12).
  bool done = false;
  while (budgetMs > 0) {
    uint32_t slice = budgetMs > 100 ? 100 : budgetMs;
    if (uart_wait_tx_done(RS485_UART_NUM, pdMS_TO_TICKS(slice)) == ESP_OK) { done = true; break; }
    budgetMs -= slice;
    rtuAlive();
  }
  if (!done) delayMicroseconds(g_charTimeUs);      // fallback: one full character time
#if (RS485_DE_PIN >= 0) && !RS485_USE_HW_DE
  delayMicroseconds(g_txGuardUs);                  // stop bit fully on the wire
  digitalWrite(RS485_DE_PIN, LOW);
#endif
}

// Expected RTU response length from what has been received so far.
// -1 = not yet known / function code with no fixed rule (fall back to t3.5 gap).
static int expectedRtuLen(size_t reqLen, const uint8_t* rx, size_t n) {
  if (n < 2) return -1;
  uint8_t fc = rx[1];
  int len = -1;
  if (fc & 0x80) return 5;                           // exception: unit fc code crc crc
  switch (fc) {
    case 1: case 2: case 3: case 4: case 12: case 17: case 20: case 21: case 23:
      if (n >= 3) len = 5 + rx[2];                   // unit fc bytecount data.. crc crc
      break;
    case 5: case 6: case 11: case 15: case 16: len = 8;  break;
    case 7:  len = 5;  break;
    case 22: len = 10; break;
    case 8:  len = (int)reqLen + 2; break;           // diagnostics echo
    case 24: {                                   // Read FIFO Queue: 2-byte byte count
      if (n < 4) break;
      int byteCount = ((int)rx[2] << 8) | rx[3]; // = 2 * FIFO count + 2
      // Spec limit: FIFO count <= 31, so byteCount <= 64 and always even.
      if (byteCount < 2 || byteCount > 64 || (byteCount & 1)) return -1;
      len = 6 + byteCount;
      break;
    }
    default: break;
  }
  return (len > 0 && len <= (int)MB_RTU_MAX) ? len : -1;
}

// Every failure mode is distinct: the exception code and the counter a field
// engineer sees must tell "no answer" apart from "answer was unusable" and
// from "somebody else answered" (audit FIX-03).
enum RtuResult {
  RTU_OK,
  RTU_BROADCAST,
  RTU_TIMEOUT,          // not a single byte came back
  RTU_CRC,              // complete, correctly framed, CRC wrong
  RTU_FRAME_ERROR,      // too short / extra bytes / no silence / RX overflow
  RTU_UNIT_MISMATCH,    // a different unit id answered
  RTU_FC_MISMATCH       // answer to a different function code
};

static void waitBusFree() {
  for (;;) {
    int32_t remaining = (int32_t)(g_busFreeAtUs - micros());
    if (remaining <= 0) return;
    if (remaining > 2000) { rtuAlive(); vTaskDelay(1); }
    else { delayMicroseconds((uint32_t)remaining); return; }
  }
}

// req = unit + fc + data (no CRC). rx receives the full RTU reply incl. CRC.
static RtuResult rtuTransaction(const uint8_t* req, size_t reqLen, uint8_t* rx, size_t& rxLen) {
  static uint8_t frame[MB_RTU_MAX];
  rxLen = 0;
  memcpy(frame, req, reqLen);
  uint16_t crc = modbusCRC(frame, reqLen);
  frame[reqLen]     = (uint8_t)(crc & 0xFF);
  frame[reqLen + 1] = (uint8_t)(crc >> 8);
  const size_t frameLen = reqLen + 2;
  const bool broadcast = (req[0] == 0);

  if (g_rtuResyncNeeded) {                        // recover from an abandoned job
    g_rtuResyncNeeded = false;
    drainUntilSilent(100);
#if (RS485_DE_PIN >= 0) && !RS485_USE_HW_DE
    digitalWrite(RS485_DE_PIN, LOW);              // guarantee receive mode
#endif
    g_busFreeAtUs = micros() + g_t35Us;
  }
  waitBusFree();
  while (rs485.available() > 0) rs485.read();      // discard stale / late bytes

  rs485TxBegin();
  rs485.write(frame, frameLen);
  rs485TxEnd(frameLen);
  const uint32_t txDoneUs = micros();

  if (broadcast) {                                  // slaves never answer unit 0
    g_busFreeAtUs = micros() + BROADCAST_TURNAROUND_MS * 1000UL;
    return RTU_BROADCAST;
  }

  // Silence-based framing. NOTE (audit FIX-09): t1.5 (750 us above 19200 baud)
  // cannot be observed from software at a 1 ms tick, so a gap *inside* a frame
  // is not detected as such — the CRC plus the exact-length + t3.5 silence
  // check below is what protects the frame. Documented limitation.
  const uint32_t gapMs   = max<uint32_t>(RS485_MIN_GAP_MS, (g_t35Us * 2) / 1000 + 2);
  const uint32_t stallMs = gapMs + (120UL * g_charTimeUs) / 1000;  // tolerance once length is known
  const uint32_t start   = millis();
  uint32_t lastByteMs = start;
  int  expected  = -1;
  bool overflow  = false;
  bool extraByte = false;
  bool silenceObserved = false;      // t3.5 of silence already proven at the boundary
  bool gapViolation = false;         // inter-character gap longer than t1.5
#if RTU_GAP_CHECK
  uint32_t lastByteUs = txDoneUs;
#endif

  // This wait blocks ONLY RtuTask (which owns nothing but the UART).
  // NetTask keeps serving TCP / HTTP / accept / health checks meanwhile.
  while (millis() - start < cfg.rtuTimeoutMs) {
    rtuAlive();
    int avail = rs485.available();
    if (avail > 0) {
      const uint32_t nowUs = micros();
      if (rxLen == 0) {                              // first byte: record turnaround
        uint32_t t = nowUs - txDoneUs;
        g_turnaroundLastUs = t;
        if (t > g_turnaroundMaxUs) g_turnaroundMaxUs = t;
        if (t < g_turnaroundMinUs) g_turnaroundMinUs = t;
      }
#if RTU_GAP_CHECK
      else {
        // Modbus RTU: a gap longer than t1.5 inside a frame makes the frame
        // invalid. What we can observe is limited by the tick and by the UART
        // driver's batching, so the check uses a floor (RTU_GAP_FLOOR_US) and
        // is effectively advisory above 19200 baud (audit FIX-A3).
        const uint32_t limit = (g_t15Us > RTU_GAP_FLOOR_US) ? g_t15Us : RTU_GAP_FLOOR_US;
        if ((uint32_t)(nowUs - lastByteUs) > limit) gapViolation = true;
      }
#endif
#if RTU_GAP_CHECK
      lastByteUs = nowUs;
#endif
      while (avail-- > 0) {
        int b = rs485.read();
        if (b < 0) break;
        if (rxLen < MB_RTU_MAX) rx[rxLen++] = (uint8_t)b; else overflow = true;
      }
      lastByteMs = millis();
      if (expected < 0) expected = expectedRtuLen(reqLen, rx, rxLen);
      if (expected > 0 && rxLen >= (size_t)expected) {
        // A byte count is a hint, not a frame delimiter: the frame is only
        // complete once the line has been silent for t3.5 (audit FIX-02).
        // Do not spin for the whole window: t3.5 is 1750 us above 19200 baud
        // but ~32 ms at 1200, and this loop takes the UART lock every pass
        // (audit FIX-A02).
        const uint32_t quietStart = micros();
        const bool yieldWhileWaiting = (g_t35Us > 3000);
        while ((int32_t)(micros() - quietStart) < (int32_t)g_t35Us) {
          if (rs485.available() > 0) break;
          if (yieldWhileWaiting) { rtuAlive(); vTaskDelay(1); }
        }
        if (rs485.available() > 0) extraByte = true;   // somebody is still talking
        else                       silenceObserved = true;
        break;
      }
      continue;
    }
    if (rxLen > 0) {
      uint32_t silent = millis() - lastByteMs;
      if (expected < 0 && silent > gapMs)   { silenceObserved = true; break; }
      if (expected > 0 && silent > stallMs) break;
    }
    vTaskDelay(1);
  }

  // Classify. Anything that is not a complete, correctly framed, CRC-valid
  // answer from the addressed unit gets its own result code (audit FIX-03).
  RtuResult result;
  if (rxLen == 0) {
    result = RTU_TIMEOUT;
  } else if (overflow || extraByte || gapViolation || rxLen < 4) {
    result = RTU_FRAME_ERROR;
  } else if (expected > 0 && rxLen != (size_t)expected) {
    result = RTU_FRAME_ERROR;                       // short or over-long frame
  } else {
    uint16_t calc = modbusCRC(rx, rxLen - 2);
    uint16_t recv = (uint16_t)(rx[rxLen - 2] | ((uint16_t)rx[rxLen - 1] << 8));
    if (calc != recv)                     result = RTU_CRC;
    else if (rx[0] != req[0])             result = RTU_UNIT_MISMATCH;
    else if ((rx[1] & 0x7F) != req[1])    result = RTU_FC_MISMATCH;
    else                                  result = RTU_OK;   // incl. slave exception replies
  }

  if (result != RTU_OK) {
    drainUntilSilent(50);                           // never let leftovers start the next frame
    silenceObserved = false;
  }
  // t3.5 is required ONCE between frames. When the frame boundary was already
  // proven by t3.5 of silence, waiting it again just wastes bus time - at 1200
  // baud that was 32 ms on every single poll (audit FIX-A03).
  g_busFreeAtUs = micros() + (silenceObserved ? 0 : g_t35Us);
  return result;
}

// --------------------------------------------------------------------------
// Asynchronous RTU engine.
// RS-485 is half-duplex, so exactly ONE job exists. Ownership of g_job is
// handed over through the queues (queue send/receive are memory barriers):
//   NetTask fills g_job -> xQueueSend(jobQ)  => RtuTask owns it
//   RtuTask fills result -> xQueueSend(doneQ) => NetTask owns it again
// NetTask never waits for the serial line; it polls doneQ with 0 timeout.
// --------------------------------------------------------------------------
struct RtuJob {
  uint32_t  jobId;               // unique, monotonic: identifies THIS transaction
  // request / response (touched by the current owner only)
  uint8_t   req[MB_RTU_MAX];     // unit + fc + data (no CRC)
  uint16_t  reqLen;
  uint8_t   rx[MB_RTU_MAX];      // full RTU reply incl. CRC
  uint16_t  rxLen;
  RtuResult result;
  // NetTask bookkeeping
  int8_t    slot;
  uint32_t  gen;
  uint8_t   tidHi, tidLo, unit, fc;
};
static RtuJob g_job;

// NetTask's view of the shared job. IDLE is the ONLY state in which g_job may
// be written; ABANDONED means the job is overdue and its client has already
// been answered, but RtuTask may still be using the buffer, so it stays
// untouched until the late completion arrives and is recognised by its id.
enum JobState { JOB_IDLE, JOB_INFLIGHT, JOB_ABANDONED };
static JobState g_jobState      = JOB_IDLE;
static uint32_t g_jobSeq        = 0;          // next job id
static uint32_t g_jobDeadlineMs = 0;
static uint8_t  g_jobLost       = 0;

static void rtuTask(void* arg) {
  g_rtuTwdtSubscribed = (esp_task_wdt_add(nullptr) == ESP_OK);
  for (;;) {
    rtuAlive();
    uint32_t jobId;
    if (xQueueReceive(g_rtuJobQ, &jobId, pdMS_TO_TICKS(200)) != pdTRUE) continue;
    size_t n = 0;
    g_job.result = rtuTransaction(g_job.req, g_job.reqLen, g_job.rx, n);
    g_job.rxLen  = (uint16_t)n;
    rtuAlive();
    xQueueSend(g_rtuDoneQ, &jobId, portMAX_DELAY);   // the id travels back with it
  }
}

// ==========================================================================
// 10. MODBUS TCP SERVER  (NetTask only)
// ==========================================================================
static void acceptModbusClients() {
  for (int guard = 0; guard < MAX_SOCK_NUM; guard++) {
    EthernetClient nc = modbusServer.accept();      // each connection returned ONCE
    if (!nc) return;
    forgetStaleHolders(nc.getSocketNumber());
    nc.setConnectionTimeout(SOCKET_CLOSE_TIMEOUT_MS);

#if MODBUS_ALLOWLIST_ENABLED
    {
      const IPAddress allow[] = MODBUS_ALLOWLIST;
      IPAddress peer = nc.remoteIP();
      bool permitted = false;
      for (size_t a = 0; a < sizeof(allow) / sizeof(allow[0]); a++)
        if (allow[a] != IPAddress(0, 0, 0, 0) && allow[a] == peer) { permitted = true; break; }
      if (!permitted) {
        LOGF("[MB] connection from %u.%u.%u.%u rejected (not in allowlist)\n",
             peer[0], peer[1], peer[2], peer[3]);
        nc.stop(); g_stat.tcpRejected++;
        continue;
      }
    }
#endif

    int slot = -1;
    for (int i = 0; i < MAX_CLIENTS; i++) if (!g_mb[i].active) { slot = i; break; }
#if EVICT_OLDEST_WHEN_FULL
    // Only a STALE session may be evicted, and never one with a request on the
    // bus. "Oldest" is not the same as "dead": a historian that polls once a
    // minute is always the oldest, and dropping it caused eviction storms in
    // the field (audit FIX-05).
    if (slot < 0) {
      uint32_t now = millis(), oldestAge = 0;
      int cand = -1;
      for (int i = 0; i < MAX_CLIENTS; i++) {
        if (g_mb[i].busy || g_mb[i].qCount > 0) continue;   // never evict pending work
        uint32_t age = now - g_mb[i].lastRxMs;
        if (cand < 0 || age > oldestAge) { cand = i; oldestAge = age; }
      }
      if (cand >= 0 && oldestAge > EVICT_IDLE_THRESHOLD_MS) {
        LOGF("[MB] evicting stale slot %d (idle %lu ms)\n", cand, (unsigned long)oldestAge);
        closeMbSession(g_mb[cand]);
        g_stat.tcpEvicted++;
        slot = cand;
      }
    }
#endif
    if (slot < 0) {
      LOGF("[MB] slots full -> rejecting connection\n");
      nc.stop();
      g_stat.tcpRejected++;
      continue;
    }
    MbSession& s = g_mb[slot];
    resetMbSlot(s);                                 // new generation for this slot
    s.sock = nc;
    s.active = true;
    s.lastRxMs = millis();
    s.frameStartMs = s.lastRxMs;
    g_stat.tcpAccepted++;
    LOGF("[MB] client on socket %u -> slot %d\n", nc.getSocketNumber(), slot);
  }
}

static void sendModbusException(MbSession& s, uint8_t tidHi, uint8_t tidLo, uint8_t unit,
                                uint8_t fc, uint8_t code) {
  uint8_t r[9] = { tidHi, tidLo, 0, 0, 0, 3, unit, (uint8_t)(fc | 0x80), code };
  if (!writeAll(s.sock, r, sizeof(r))) { closeMbSession(s); g_stat.tcpDropped++; }
}

// Function-code audit table (audit FIX-07 / FIX-20). "req" is unit+fc+data, no
// CRC; "resp" is the RTU response including CRC. Anything not listed is passed
// through untouched, because a transparent gateway must not block a vendor
// function it does not know.
//
//  FC    request len   response len              limits enforced here
//  01    6             5 + byteCount             qty 1..2000
//  02    6             5 + byteCount             qty 1..2000
//  03    6             5 + byteCount             qty 1..125
//  04    6             5 + byteCount             qty 1..125
//  05    6             8 (echo)                  -
//  06    6             8 (echo)                  -
//  07    2             5                         -
//  08    6             req + 2 (echo)            -
//  0B    2             8                         -
//  0C    2             5 + byteCount             -
//  0F    7 + bc        8                         qty 1..1968, bc == ceil(qty/8)
//  10    7 + bc        8                         qty 1..123,  bc == 2*qty
//  11    2             5 + byteCount             -
//  14    variable      5 + byteCount             pass-through
//  15    variable      echo (5 + len)            pass-through
//  16    8             10                        -
//  17    11 + bc       5 + byteCount             read 1..125, write 1..121
//  18    4             6 + byteCount(2 bytes)    FIFO count <= 31, even count
//  2B    variable      variable                  pass-through, silence framing
//  exception           5                         any fc | 0x80
//
// Reject requests that no slave could ever answer, before they occupy the
// half-duplex bus for a full timeout (audit FIX-08).
// Returns 0 if the request may go out, otherwise the Modbus exception code.
static uint8_t validateRtuRequest(const uint8_t* req, size_t len) {
  if (len < 2) return 0x03;                       // illegal data value
  const uint8_t fc = req[1];
  if (fc == 0x00 || fc >= 0x80) return 0x01;      // illegal function

#if !STRICT_REQUEST_VALIDATION
  return 0;                                       // transparent mode: pass everything else
#else

  auto be16 = [&](size_t i) -> uint16_t { return (uint16_t)((req[i] << 8) | req[i + 1]); };

  switch (fc) {
    case 0x01: case 0x02: {                       // read coils / discrete inputs
      if (len != 6) return 0x03;
      uint16_t qty = be16(4);
      if (qty < 1 || qty > 2000) return 0x03;
      return 0;
    }
    case 0x03: case 0x04: {                       // read holding / input registers
      if (len != 6) return 0x03;
      uint16_t qty = be16(4);
      if (qty < 1 || qty > 125) return 0x03;
      return 0;
    }
    case 0x05: case 0x06:                         // write single coil / register
      return (len == 6) ? 0 : 0x03;
    case 0x07: case 0x0B: case 0x0C: case 0x11:   // no-data requests
      return (len == 2) ? 0 : 0x03;
    case 0x08:                                    // diagnostics
      return (len == 6) ? 0 : 0x03;
    case 0x0F: {                                  // write multiple coils
      if (len < 8) return 0x03;
      uint16_t qty = be16(4);
      uint8_t  bc  = req[6];
      if (qty < 1 || qty > 1968 || bc != (uint8_t)((qty + 7) / 8) || len != (size_t)(7 + bc))
        return 0x03;
      return 0;
    }
    case 0x10: {                                  // write multiple registers
      if (len < 8) return 0x03;
      uint16_t qty = be16(4);
      uint8_t  bc  = req[6];
      if (qty < 1 || qty > 123 || bc != (uint8_t)(qty * 2) || len != (size_t)(7 + bc))
        return 0x03;
      return 0;
    }
    case 0x16:                                    // mask write register
      return (len == 8) ? 0 : 0x03;
    case 0x17: {                                  // read/write multiple registers
      if (len < 12) return 0x03;
      uint16_t rQty = be16(4), wQty = be16(8);
      uint8_t  bc   = req[10];
      if (rQty < 1 || rQty > 125 || wQty < 1 || wQty > 121 ||
          bc != (uint8_t)(wQty * 2) || len != (size_t)(11 + bc))
        return 0x03;
      return 0;
    }
    case 0x18:                                    // read FIFO queue
      return (len == 4) ? 0 : 0x03;
    default:
      return 0;                                   // file records, encapsulated, vendor: pass through
  }
#endif
}

// Non-blocking: hand the next queued request (round-robin over clients) to
// RtuTask. The RS-485 bus stays strictly serialized; the TCP side does not.
static void dispatchRtuJob() {
  static uint8_t rr = 0;
  if (g_jobState != JOB_IDLE) return;              // g_job belongs to RtuTask

  for (int k = 0; k < MAX_CLIENTS; k++) {
    int i = (rr + k) % MAX_CLIENTS;
    MbSession& s = g_mb[i];
    if (!s.active || s.busy || s.qCount == 0) continue;

    PendingReq& r = s.q[s.qHead];

    // A request that waited longer than the master's patience is answered
    // rather than put on the bus (audit FIX-A4 / queue ageing).
    if (millis() - r.enqueuedMs > REQUEST_MAX_AGE_MS) {
      LOGF("[MB] request aged out in queue (unit %u fc %u)\n", r.unit, r.fc);
      uint8_t tidHi = r.tidHi, tidLo = r.tidLo, unit = r.unit, fc = r.fc;
      s.qHead = (uint8_t)((s.qHead + 1) % REQ_QUEUE_DEPTH);
      s.qCount--;
      g_stat.reqAged++;
      if (cfg.flags & CFG_FLAG_TCP_EXCEPTION)
        sendModbusException(s, tidHi, tidLo, unit, fc, 0x0A);   // gateway path unavailable
      rr = (uint8_t)((i + 1) % MAX_CLIENTS);
      return;
    }

    g_job.jobId  = ++g_jobSeq;
    g_job.tidHi  = r.tidHi;
    g_job.tidLo  = r.tidLo;
    g_job.unit   = r.unit;
    g_job.fc     = r.fc;
    g_job.reqLen = r.rtuLen;
    memcpy(g_job.req, r.rtu, r.rtuLen);
    g_job.slot   = (int8_t)i;
    g_job.gen    = s.gen;

    s.qHead = (uint8_t)((s.qHead + 1) % REQ_QUEUE_DEPTH);
    s.qCount--;
    s.busy = true;                                 // keep this client's answers in order

    g_stat.requests++;
    g_lastTrafficMs = millis();

    uint32_t jobId = g_job.jobId;
    if (xQueueSend(g_rtuJobQ, &jobId, 0) != pdTRUE) {   // cannot happen: depth 1, state IDLE
      LOGF("[SYS] RTU job queue refused a job\n");
      s.busy = false;
      g_stat.internalErr++;
      if (cfg.flags & CFG_FLAG_TCP_EXCEPTION)
        sendModbusException(s, g_job.tidHi, g_job.tidLo, g_job.unit, g_job.fc, 0x0A);
      return;
    }
    g_jobState      = JOB_INFLIGHT;
    g_jobDeadlineMs = millis() + cfg.rtuTimeoutMs + RTU_JOB_GRACE_MS;
    rr = (uint8_t)((i + 1) % MAX_CLIENTS);
    return;
  }
}

// Non-blocking: if RtuTask finished, send the reply to the client that asked
// (if that client is still the same connection).
//
// Exception mapping (audit FIX-03) — a field engineer must be able to tell
// these apart from the master's side:
//   0x0B Gateway target device failed to respond : nothing came back at all
//   0x04 Server device failure                   : the slave answered, but the
//                                                  answer was unusable (CRC /
//                                                  framing) -> suspect baud,
//                                                  parity, wiring, noise
//   0x0A Gateway path unavailable                : somebody else answered
//                                                  (duplicate unit id, bus
//                                                  contention), the request
//                                                  aged out, or the gateway
//                                                  itself failed internally
static void completeRtuJob() {
  if (g_jobState == JOB_IDLE) return;

  uint32_t doneId = 0;
  if (xQueueReceive(g_rtuDoneQ, &doneId, 0) != pdTRUE) {
    // Overdue. Answer the client now, but do NOT release g_job: RtuTask may
    // still be reading it, and a new request must never overwrite a buffer
    // whose owner has not finished with it (audit FIX-A1).
    if ((int32_t)(millis() - g_jobDeadlineMs) > 0) {
      if (g_jobState == JOB_INFLIGHT) {
        g_stat.internalErr++;
        MbSession& ls = g_mb[g_job.slot];
        if (ls.active && ls.gen == g_job.gen) {
          ls.busy = false;
          if (cfg.flags & CFG_FLAG_TCP_EXCEPTION)
            sendModbusException(ls, g_job.tidHi, g_job.tidLo, g_job.unit, g_job.fc, 0x0A);
        }
        g_jobState = JOB_ABANDONED;                 // buffer stays frozen
        g_rtuResyncNeeded = true;                   // next job re-syncs the bus
        g_stat.rtuAbandoned++;
        g_jobDeadlineMs = millis() + RTU_JOB_GRACE_MS;
        LOGF("[SYS] RTU job %lu overdue -> abandoned, waiting for RtuTask\n",
             (unsigned long)g_job.jobId);
      } else {
        // Still nothing after the grace period: RtuTask is not running. The bus
        // is unusable and no new job may ever be dispatched, so restart.
        if (++g_jobLost >= RTU_JOB_LOST_LIMIT) {
          LOGF("[SYS] RtuTask unresponsive (%u abandoned jobs) -> restarting\n", g_jobLost);
          Serial.flush();
          esp_restart();
        }
        g_jobDeadlineMs = millis() + RTU_JOB_GRACE_MS;
      }
    }
    return;
  }

  // A completion only counts for the job it names.
  if (doneId != g_job.jobId) {
    g_stat.staleResults++;
    LOGF("[SYS] discarding completion for job %lu (current %lu)\n",
         (unsigned long)doneId, (unsigned long)g_job.jobId);
    return;
  }

  if (g_jobState == JOB_ABANDONED) {                // late answer to a dead request
    g_stat.staleResults++;
    g_jobState = JOB_IDLE;
    g_jobLost = 0;
    LOGF("[SYS] late result for abandoned job %lu discarded\n", (unsigned long)doneId);
    return;
  }

  g_jobState = JOB_IDLE;
  g_jobLost = 0;
  g_lastTrafficMs = millis();

  const RtuJob& j = g_job;
  MbSession& s = g_mb[j.slot];
  const bool alive = s.active && s.gen == j.gen;    // client may have gone meanwhile
  if (alive) s.busy = false;

  uint8_t exception = 0;
  switch (j.result) {
    case RTU_OK: {
      g_stat.rtuOk++;
      unitStatRecord(j.unit, true);
      if (!alive) break;
      static uint8_t tx[MB_TCP_ADU_MAX + 2];
      size_t pduLen = j.rxLen - 2;                  // unit + fc + data
      tx[0] = j.tidHi; tx[1] = j.tidLo;             // transaction id, exactly as received
      tx[2] = 0;       tx[3] = 0;                   // protocol id
      tx[4] = (uint8_t)(pduLen >> 8);
      tx[5] = (uint8_t)(pduLen & 0xFF);
      memcpy(&tx[6], j.rx, pduLen);
      if (!writeAll(s.sock, tx, pduLen + 6)) { closeMbSession(s); g_stat.tcpDropped++; }
      break;
    }
    case RTU_BROADCAST:
      g_stat.broadcasts++;                          // no response for broadcast
      break;
    case RTU_TIMEOUT:
      unitStatRecord(j.unit, false);
      g_stat.rtuTimeout++;
      LOGF("[RTU] timeout unit %u fc %u\n", j.unit, j.fc);
      exception = 0x0B;
      break;
    case RTU_CRC:
      unitStatRecord(j.unit, false);
      g_stat.rtuCrc++;
      LOGF("[RTU] CRC error unit %u fc %u (%u bytes)\n", j.unit, j.fc, (unsigned)j.rxLen);
      exception = 0x04;
      break;
    case RTU_FRAME_ERROR:
      unitStatRecord(j.unit, false);
      g_stat.rtuFrame++;
      LOGF("[RTU] frame error unit %u fc %u (%u bytes: short/extra/gap/overflow)\n",
           j.unit, j.fc, (unsigned)j.rxLen);
      exception = 0x04;
      break;
    case RTU_UNIT_MISMATCH:
      unitStatRecord(j.unit, false);
      g_stat.rtuUnitMismatch++;
      LOGF("[RTU] unit %u asked, unit %u answered (duplicate address?)\n", j.unit, j.rx[0]);
      exception = 0x0A;
      break;
    case RTU_FC_MISMATCH:
      unitStatRecord(j.unit, false);
      g_stat.rtuFcMismatch++;
      LOGF("[RTU] fc %u asked, fc %u answered\n", j.fc, j.rx[1]);
      exception = 0x0A;
      break;
  }
  // MOXA's "Modbus TCP Exception" switch: some masters prefer no answer at all
  // and their own timeout, and treat an exception as a device fault (FIX-A12).
  if (exception && alive && (cfg.flags & CFG_FLAG_TCP_EXCEPTION))
    sendModbusException(s, j.tidHi, j.tidLo, j.unit, j.fc, exception);
}

// Non-blocking: read bytes, cut the stream into Modbus ADUs, queue them.
// TCP is a byte stream: one read may contain several ADUs, half an ADU, or the
// tail of one plus the head of the next. All three are handled here.
static void serviceModbusSessions() {
  for (int i = 0; i < MAX_CLIENTS; i++) {
    MbSession& s = g_mb[i];
    if (!s.active) continue;

    if (!s.sock.connected()) { closeMbSession(s); g_stat.tcpDisconnects++; continue; }

    uint32_t now = millis();
    int avail = s.sock.available();
    if (avail > 0 && s.len < MB_TCP_ADU_MAX) {
      size_t room = MB_TCP_ADU_MAX - s.len;
      size_t want = (size_t)avail < room ? (size_t)avail : room;
      int n = s.sock.read(s.buf + s.len, want);
      if (n > 0) {
        if (s.len == 0) s.frameStartMs = now;
        s.len += (uint16_t)n;
        s.lastRxMs = now;
      }
    }

    // Cut out every complete ADU the ring can still hold.
    bool blockedByQueue = false;
    bool closed = false;
    while (s.len >= 7) {
      uint16_t protocolId  = ((uint16_t)s.buf[2] << 8) | s.buf[3];
      uint16_t declaredLen = ((uint16_t)s.buf[4] << 8) | s.buf[5];
      if (protocolId != 0 || declaredLen < 2 || declaredLen > 254) {
        LOGF("[MB] bad MBAP header -> closing client\n");
        closeMbSession(s); g_stat.tcpDropped++;
        closed = true;
        break;
      }
      size_t aduLen = 6 + declaredLen;
      if (s.len < aduLen) break;                    // wait for the rest
      if (s.qCount >= REQ_QUEUE_DEPTH) {            // back-pressure, nothing dropped
        blockedByQueue = true;
        g_stat.queueBackpressure++;
        break;
      }

      const uint8_t tidHi = s.buf[0], tidLo = s.buf[1];
      const uint8_t unit  = s.buf[6], fc = s.buf[7];

      // Reject what no slave could answer before it ever reaches the bus.
      uint8_t reject = validateRtuRequest(s.buf + 6, declaredLen);
      if (reject) {
        LOGF("[MB] rejecting request unit %u fc %u -> exception 0x%02X\n", unit, fc, reject);
        g_stat.reqRejected++;
        sendModbusException(s, tidHi, tidLo, unit, fc, reject);
      } else {
        uint8_t tail = (uint8_t)((s.qHead + s.qCount) % REQ_QUEUE_DEPTH);
        PendingReq& r = s.q[tail];
        r.tidHi = tidHi; r.tidLo = tidLo;
        r.unit  = unit;  r.fc = fc;
        r.rtuLen = declaredLen;                     // unit + pdu
        memcpy(r.rtu, s.buf + 6, declaredLen);
        r.enqueuedMs = now;
        s.qCount++;
      }

      s.len -= (uint16_t)aduLen;
      if (s.len) memmove(s.buf, s.buf + aduLen, s.len);
      s.frameStartMs = now;
      if (!s.active) { closed = true; break; }      // write failure inside the reject path
    }
    if (closed) continue;

    // An incomplete ADU must not sit forever; a complete one waiting for the
    // bus, or bytes held back by a full ring, are not "incomplete".
    if (!blockedByQueue && s.len > 0 && now - s.frameStartMs > TCP_PARTIAL_FRAME_TIMEOUT_MS) {
      LOGF("[MB] incomplete frame timeout -> closing client\n");
      closeMbSession(s); g_stat.tcpDropped++;
      continue;
    }
#if CLIENT_IDLE_TIMEOUT_MS > 0
    if (!s.busy && s.qCount == 0 && s.len == 0 && now - s.lastRxMs > CLIENT_IDLE_TIMEOUT_MS) {
      LOGF("[MB] idle timeout -> closing client\n");
      closeMbSession(s);
    }
#endif
  }
}

// Boot/reset reason: the first thing a field engineer wants after an unexpected
// restart (audit FIX-38).
static const char* resetReasonName() {
  switch (esp_reset_reason()) {
    case ESP_RST_POWERON:  return "power-on";
    case ESP_RST_EXT:      return "external pin";
    case ESP_RST_SW:       return "software restart";
    case ESP_RST_PANIC:    return "panic / exception";
    case ESP_RST_INT_WDT:  return "interrupt watchdog";
    case ESP_RST_TASK_WDT: return "task watchdog";
    case ESP_RST_WDT:      return "other watchdog";
    case ESP_RST_BROWNOUT: return "brown-out";
    case ESP_RST_SDIO:     return "SDIO";
    default:               return "unknown";
  }
}

static uint8_t queuedRequests() {
  uint8_t n = 0;
  for (auto& s : g_mb) if (s.active) n = (uint8_t)(n + s.qCount);
  return n;
}

static uint8_t activeModbusClients() {
  uint8_t n = 0;
  for (auto& s : g_mb) if (s.active) n++;
  return n;
}

// ==========================================================================
// 11. HTTP CONFIG SERVER  (NetTask only, zero heap)
// ==========================================================================
// A ring of tokens: opening a second configuration tab must not invalidate the
// first one's form, while every token still expires and is single-use (FIX-02).
struct CsrfToken { char value[17]; uint32_t issuedMs; bool used; };
static CsrfToken g_csrf[CSRF_TOKEN_SLOTS] = {};

// Per-source-IP brute-force lockout (audit FIX-22): one attacker must not be
// able to lock the real administrator out.
struct AuthHost { uint32_t ip; uint8_t fails; uint32_t lockUntil; };
static AuthHost g_authHosts[AUTH_TRACKED_HOSTS] = {};

// A per-IP table alone is bypassed by an attacker who rotates source addresses,
// so a global cap runs alongside it (audit FIX-A06).
#define AUTH_GLOBAL_WINDOW_MS 60000UL
#define AUTH_GLOBAL_MAX_FAILS 20
static uint16_t g_authFailWindow  = 0;
static uint32_t g_authWindowStart = 0;
// The configuration page carries the form plus ~25 diagnostic rows plus the
// per-slave table, so it needs real room. Static, so the request path still
// allocates nothing; 12 kB of an ESP32's ~300 kB is affordable (audit FIX-62).
#define PAGE_BUFFER_SIZE 12288
static char     g_page[PAGE_BUFFER_SIZE];
static size_t   g_pageLen = 0;
static bool     g_pageOverflow = false;
static size_t   g_pageNeeded = 0;          // bytes the last build would have used

// Credentials live inside the configuration record, so a password change is the
// same single atomic write as a settings change (audit FIX-A2).
static void authReport() {
  if (!g_pwIsDefault) return;
#if WEB_FORCE_DEFAULT_PASSWORD
  char def[24];
  defaultPassword(def, sizeof(def));
  LOGF("[SEC] web login: %s / %s\n", WEB_AUTH_USER, def);
  LOGF("[SEC] WEB_FORCE_DEFAULT_PASSWORD is 1: this password is re-applied at "
       "every boot and printed here. Set it to 0 before shipping.\n");
#else
  LOGF("[SEC] web login: the built-in default password is active - change it "
       "from the web page\n");
#endif
}

static AuthHost* authSlot(uint32_t ip) {
  for (auto& h : g_authHosts) if (h.ip == ip) return &h;
  AuthHost* oldest = &g_authHosts[0];
  for (auto& h : g_authHosts) {
    if (h.ip == 0) { h.ip = ip; h.fails = 0; h.lockUntil = 0; return &h; }
    if ((int32_t)(h.lockUntil - oldest->lockUntil) < 0) oldest = &h;
  }
  oldest->ip = ip; oldest->fails = 0; oldest->lockUntil = 0;
  return oldest;
}

// Issue a token into the oldest / expired / already used slot.
static const char* newCsrfToken() {
  const uint32_t now = millis();
  int victim = 0;
  for (int i = 0; i < CSRF_TOKEN_SLOTS; i++) {
    CsrfToken& t = g_csrf[i];
    if (t.value[0] == 0 || t.used || now - t.issuedMs > CSRF_TOKEN_TTL_MS) { victim = i; break; }
    if ((int32_t)(t.issuedMs - g_csrf[victim].issuedMs) < 0) victim = i;
  }
  CsrfToken& t = g_csrf[victim];
  snprintf(t.value, sizeof(t.value), "%08lX%08lX",
           (unsigned long)esp_random(), (unsigned long)esp_random());
  t.issuedMs = now;
  t.used = false;
  return t.value;
}

// Accept any unexpired, unused token; consume it so it cannot be replayed.
static bool csrfValid(const char* token) {
  if (!token || strlen(token) != 16) return false;
  const uint32_t now = millis();
  for (auto& t : g_csrf) {
    if (t.value[0] == 0 || t.used) continue;
    if (now - t.issuedMs > CSRF_TOKEN_TTL_MS) { t.value[0] = 0; continue; }
    if (ctEqualBytes((const uint8_t*)token, (const uint8_t*)t.value, 16)) {
      t.used = true;                                  // single use
      return true;
    }
  }
  return false;
}

static void pageReset() {
  g_pageLen = 0; g_pageOverflow = false; g_pageNeeded = 0; g_page[0] = '\0';
}

static void pageRaw(const char* s) {                 // literal text (may contain '%')
  size_t n = strlen(s);
  g_pageNeeded += n;
  if (g_pageLen + n >= sizeof(g_page)) { g_pageOverflow = true; return; }
  memcpy(g_page + g_pageLen, s, n + 1);
  g_pageLen += n;
}

static void pageFmt(const char* fmt, ...) __attribute__((format(printf, 1, 2)));
static void pageFmt(const char* fmt, ...) {
  if (g_pageLen >= sizeof(g_page) - 1) { g_pageOverflow = true; return; }
  va_list ap;
  va_start(ap, fmt);
  int n = vsnprintf(g_page + g_pageLen, sizeof(g_page) - g_pageLen, fmt, ap);
  va_end(ap);
  if (n >= 0) g_pageNeeded += (size_t)n;
  if (n < 0 || (size_t)n >= sizeof(g_page) - g_pageLen) {
    g_pageOverflow = true;
    g_pageLen = sizeof(g_page) - 1;
  } else {
    g_pageLen += (size_t)n;
  }
}

// A HEAD request gets exactly the headers a GET would produce, and no body.
static bool g_httpHeadOnly = false;

static void httpSend(EthernetClient& c, int code, const char* reason, const char* ctype,
                     const char* body, size_t bodyLen, const char* extraHeaders = "") {
  char hdr[320];
  int n = snprintf(hdr, sizeof(hdr),
                   "HTTP/1.1 %d %s\r\n"
                   "Content-Type: %s\r\n"
                   "Content-Length: %u\r\n"
                   "Connection: close\r\n"
                   "Cache-Control: no-store\r\n"
                   "X-Frame-Options: DENY\r\n"
                   "%s\r\n",
                   code, reason, ctype, (unsigned)bodyLen, extraHeaders);
  if (n <= 0 || (size_t)n >= sizeof(hdr)) return;
  // The config page is ~4 KB; a Modbus response is <= 260 B. One deadline does
  // not fit both (audit FIX-A04), and a browser that stops reading must not be
  // able to delay the Modbus path (audit FIX-08).
  if (!writeAll(c, (const uint8_t*)hdr, (size_t)n, HTTP_WRITE_TIMEOUT_MS)) {
    g_stat.httpWriteFail++;
    return;
  }
  if (bodyLen && !g_httpHeadOnly &&
      !writeAll(c, (const uint8_t*)body, bodyLen, HTTP_WRITE_TIMEOUT_MS))
    g_stat.httpWriteFail++;
}

static void httpSendSimple(EthernetClient& c, int code, const char* reason, const char* msg,
                           const char* extraHeaders = "") {
  pageReset();
  pageRaw("<!DOCTYPE html><html><body style='font-family:Arial;text-align:center;padding:50px;'>");
  pageFmt("<h2>%d %s</h2><p>%s</p><p><a href='/'>Back</a></p></body></html>", code, reason, msg);
  httpSend(c, code, reason, "text/html", g_page, g_pageLen, extraHeaders);
}

// Case-insensitive header lookup within the header block.
static bool httpHeader(const char* req, size_t hdrLen, const char* name, char* out, size_t outCap) {
  const char* end = req + hdrLen;
  const char* p = strstr(req, "\r\n");
  if (!p) return false;
  p += 2;
  size_t nlen = strlen(name);
  while (p < end) {
    const char* eol = strstr(p, "\r\n");
    if (!eol || eol == p || eol > end) break;
    if ((size_t)(eol - p) > nlen && strncasecmp(p, name, nlen) == 0 && p[nlen] == ':') {
      const char* v = p + nlen + 1;
      while (v < eol && (*v == ' ' || *v == '\t')) v++;
      size_t vl = (size_t)(eol - v);
      while (vl && (v[vl - 1] == ' ' || v[vl - 1] == '\t')) vl--;
      if (vl >= outCap) return false;
      memcpy(out, v, vl);
      out[vl] = '\0';
      return true;
    }
    p = eol + 2;
  }
  return false;
}

static int hexVal(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  c = (char)tolower((unsigned char)c);
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  return -1;
}

// application/x-www-form-urlencoded field lookup + decode.
static bool formField(const char* body, const char* key, char* out, size_t cap) {
  size_t klen = strlen(key);
  const char* p = body;
  while (*p) {
    const char* amp = strchr(p, '&');
    const char* end = amp ? amp : p + strlen(p);
    const char* eq = (const char*)memchr(p, '=', (size_t)(end - p));
    if (eq && (size_t)(eq - p) == klen && strncmp(p, key, klen) == 0) {
      size_t o = 0;
      for (const char* s = eq + 1; s < end; ) {
        char c = *s++;
        if (c == '+') c = ' ';
        else if (c == '%') {
          if (end - s < 2) return false;
          int h = hexVal(s[0]), l = hexVal(s[1]);
          if (h < 0 || l < 0) return false;
          c = (char)((h << 4) | l);
          s += 2;
        }
        if (o + 1 >= cap) return false;
        out[o++] = c;
      }
      out[o] = '\0';
      return true;
    }
    if (!amp) break;
    p = amp + 1;
  }
  return false;
}

// Decode the Basic credentials, hash them, compare against the stored hash.
static bool httpAuthorized(const char* req, size_t hdrLen, bool& credentialsPresent) {
  credentialsPresent = false;
  char v[200];
  if (!httpHeader(req, hdrLen, "Authorization", v, sizeof(v))) return false;
  if (strncasecmp(v, "Basic ", 6) != 0) return false;
  credentialsPresent = true;

  const char* tok = v + 6;
  while (*tok == ' ') tok++;

  uint8_t plain[WEB_PASS_MAX_LEN + 48];
  size_t plainLen = 0;
  if (mbedtls_base64_decode(plain, sizeof(plain) - 1, &plainLen,
                            (const unsigned char*)tok, strlen(tok)) != 0) return false;
  plain[plainLen] = 0;

  char* colon = strchr((char*)plain, ':');
  if (!colon) { memset(plain, 0, sizeof(plain)); return false; }
  *colon = 0;
  const char* user = (const char*)plain;
  const char* pass = colon + 1;

  bool ok = false;
  if (strcmp(user, WEB_AUTH_USER) == 0) {
    uint8_t h[32];
    hashCredentials(user, pass, h);
    ok = ctEqualBytes(h, g_pwHash, sizeof(h));
  }
  memset(plain, 0, sizeof(plain));
  memset(v, 0, sizeof(v));
  return ok;
}

static void selectOpt(const char* value, const char* label, bool sel) {
  pageFmt("<option value=\"%s\"%s>%s</option>", value, sel ? " selected" : "", label);
}

static void sendConfigPage(EthernetClient& c) {
  pageReset();
  pageRaw(
    "<!DOCTYPE html><html><head><meta charset=\"utf-8\">"
    "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
    "<title>Modbus Gateway</title><style>"
    "body{font-family:Arial,sans-serif;background:#eef2f3;padding:20px;display:flex;justify-content:center}"
    ".box{background:#fff;padding:25px;border-radius:8px;box-shadow:0 4px 10px rgba(0,0,0,.1);width:100%;max-width:450px}"
    "h2{text-align:center;color:#2c3e50;border-bottom:2px solid #3498db;padding-bottom:10px}"
    "label{font-weight:bold;margin-top:15px;display:block;color:#34495e;font-size:14px}"
    "input,select{width:100%;padding:10px;margin-top:5px;border:1px solid #bdc3c7;border-radius:4px;box-sizing:border-box}"
    ".btn{width:100%;padding:12px;background:#3498db;color:#fff;border:none;border-radius:4px;margin-top:25px;cursor:pointer;font-size:16px;font-weight:bold}"
    ".btn:hover{background:#2980b9}"
    ".st{background:#ecf0f1;padding:8px;margin-top:20px;border-radius:4px;text-align:center;color:#2980b9;font-weight:bold}"
    "table{width:100%;font-size:13px;margin-top:8px;border-collapse:collapse}td{padding:3px 4px;border-bottom:1px solid #eee}"
    "</style></head><body><div class=\"box\"><h2>Modbus Gateway Setup</h2>"
    "<form action=\"/save\" method=\"POST\">"
    "<div class=\"st\">Ethernet Network (W5500)</div>");

  char ip[16], sn[16], gw[16];
  snprintf(ip, sizeof(ip), "%u.%u.%u.%u", cfg.ip[0], cfg.ip[1], cfg.ip[2], cfg.ip[3]);
  snprintf(sn, sizeof(sn), "%u.%u.%u.%u", cfg.sn[0], cfg.sn[1], cfg.sn[2], cfg.sn[3]);
  snprintf(gw, sizeof(gw), "%u.%u.%u.%u", cfg.gw[0], cfg.gw[1], cfg.gw[2], cfg.gw[3]);
  pageFmt("<label>Static IP</label><input name=\"l_ip\" maxlength=\"15\" value=\"%s\">", ip);
  pageFmt("<label>Subnet Mask</label><input name=\"l_sn\" maxlength=\"15\" value=\"%s\">", sn);
  pageFmt("<label>Gateway</label><input name=\"l_gw\" maxlength=\"15\" value=\"%s\">", gw);
  pageFmt("<label>Modbus TCP Port</label><input type=\"number\" name=\"m_port\" min=\"1\" max=\"65535\" value=\"%u\">", cfg.port);

  pageRaw("<div class=\"st\">RS-485 Serial Settings</div><label>Baud Rate</label><select name=\"baud\">");
  for (uint8_t i = 0; i < BAUD_COUNT; i++) {
    char v[8]; snprintf(v, sizeof(v), "%lu", (unsigned long)BAUDS[i]);
    selectOpt(v, v, BAUDS[i] == cfg.baud);
  }
  pageRaw("</select><label>Data / Parity / Stop</label><select name=\"fmt\">");
  for (uint8_t i = 0; i < SERIAL_FMT_COUNT; i++) {
    char v[4]; snprintf(v, sizeof(v), "%u", i);
    selectOpt(v, SERIAL_FMTS[i].name, i == cfg.fmt);
  }
  pageFmt("</select><label>RTU Response Timeout (ms)</label>"
          "<input type=\"number\" name=\"rtu_to\" min=\"20\" max=\"5000\" value=\"%u\">", cfg.rtuTimeoutMs);

  // Suggest a timeout from what the slaves actually did (FIX-A16).
  if (g_turnaroundMaxUs > 0) {
    uint32_t suggest = (g_turnaroundMaxUs / 1000) * 3 + 20;     // 3x worst case + margin
    if (suggest < 50) suggest = 50;
    if (suggest > 5000) suggest = 5000;
    pageFmt("<p style='font-size:12px;color:#7f8c8d;margin:6px 0'>Measured slave turnaround: "
            "min %lu ms, max %lu ms &rarr; suggested timeout <b>%lu ms</b></p>",
            (unsigned long)(g_turnaroundMinUs == 0xFFFFFFFFUL ? 0 : g_turnaroundMinUs / 1000),
            (unsigned long)(g_turnaroundMaxUs / 1000), (unsigned long)suggest);
  }

  pageRaw("<label>Reply with a Modbus exception when the slave fails</label><select name=\"exc\">");
  selectOpt("1", "Yes - return exception (0x0B / 0x04 / 0x0A)", (cfg.flags & CFG_FLAG_TCP_EXCEPTION) != 0);
  selectOpt("0", "No - stay silent, let the master time out", (cfg.flags & CFG_FLAG_TCP_EXCEPTION) == 0);
  pageRaw("</select>");

  pageRaw("<div class=\"st\">Web Password</div>");
  if (g_pwIsDefault)
    pageRaw("<p style='color:#e67e22;font-size:13px;margin:8px 0'><b>This device still uses the "
            "built-in default password.</b> Set a new one below. (If the firmware was built with "
            "WEB_FORCE_DEFAULT_PASSWORD = 1, the default comes back at the next boot.)</p>");
  pageFmt("<label>New Password (%u-%u chars, blank = keep current)</label>"
          "<input type=\"password\" name=\"pw1\" maxlength=\"%u\" autocomplete=\"new-password\">"
          "<label>Repeat New Password</label>"
          "<input type=\"password\" name=\"pw2\" maxlength=\"%u\" autocomplete=\"new-password\">",
          (unsigned)WEB_PASS_MIN_LEN, (unsigned)WEB_PASS_MAX_LEN,
          (unsigned)WEB_PASS_MAX_LEN, (unsigned)WEB_PASS_MAX_LEN);

  pageFmt("<input type=\"hidden\" name=\"csrf\" value=\"%s\">", newCsrfToken());
  pageRaw("<button type=\"submit\" class=\"btn\">Save &amp; Restart</button></form>");

  pageFmt("<div class=\"st\">Status</div><table>"
          "<tr><td>Firmware</td><td>%s</td></tr>"
          "<tr><td>Last reset</td><td>%s</td></tr>"
          "<tr><td>MAC</td><td>%02X:%02X:%02X:%02X:%02X:%02X</td></tr>"
          "<tr><td>Uptime</td><td>%lu s</td></tr>"
          "<tr><td>Link / W5500</td><td>%s / %s</td></tr>"
          "<tr><td>Modbus clients</td><td>%u / %u</td></tr>"
          "<tr><td>Queued requests</td><td>%u (max %u per client)</td></tr>"
          "<tr><td>Requests / OK</td><td>%lu / %lu</td></tr>"
          "<tr><td>RTU timeout</td><td>%lu</td></tr>"
          "<tr><td>RTU CRC / frame error</td><td>%lu / %lu</td></tr>"
          "<tr><td>Wrong unit / wrong function</td><td>%lu / %lu</td></tr>"
          "<tr><td>UART frame / parity / overflow</td><td>%lu / %lu / %lu</td></tr>"
          "<tr><td>Broadcasts / rejected / aged out</td><td>%lu / %lu / %lu</td></tr>"
          "<tr><td>Stale RTU results / abandoned jobs</td><td>%lu / %lu</td></tr>"
          "<tr><td>Queue back-pressure events</td><td>%lu</td></tr>"
          "<tr><td>TCP disconnects</td><td>%lu</td></tr>"
          "<tr><td>Auth failures / lockouts</td><td>%lu / %lu</td></tr>"
          "<tr><td>HTTP write failures</td><td>%lu</td></tr>"
          "<tr><td>Config write / CRC failures</td><td>%lu / %lu</td></tr>"
          "<tr><td>TCP accepted / evicted / dropped</td><td>%lu / %lu / %lu</td></tr>"
          "<tr><td>TCP rejected / internal errors</td><td>%lu / %lu</td></tr>"
          "<tr><td>W5500 recoveries</td><td>%lu</td></tr>"
          "<tr><td>Free heap</td><td>%lu bytes</td></tr>"
          "<tr><td>Stack left Net / Rtu</td><td>%lu / %lu bytes</td></tr>"
          "<tr><td>Slave turnaround last / max</td><td>%lu / %lu ms</td></tr>"
          "</table>",
          FW_VERSION, resetReasonName(),
          mac[0], mac[1], mac[2], mac[3], mac[4], mac[5],
          (unsigned long)(millis() / 1000), g_linkUp ? "UP" : "DOWN", g_ethOk ? "OK" : "FAULT",
          activeModbusClients(), (unsigned)MAX_CLIENTS,
          queuedRequests(), (unsigned)REQ_QUEUE_DEPTH,
          (unsigned long)g_stat.requests, (unsigned long)g_stat.rtuOk,
          (unsigned long)g_stat.rtuTimeout,
          (unsigned long)g_stat.rtuCrc, (unsigned long)g_stat.rtuFrame,
          (unsigned long)g_stat.rtuUnitMismatch, (unsigned long)g_stat.rtuFcMismatch,
          (unsigned long)g_uartFrameErr, (unsigned long)g_uartParityErr, (unsigned long)g_uartOverflow,
          (unsigned long)g_stat.broadcasts, (unsigned long)g_stat.reqRejected,
          (unsigned long)g_stat.reqAged,
          (unsigned long)g_stat.staleResults, (unsigned long)g_stat.rtuAbandoned,
          (unsigned long)g_stat.queueBackpressure, (unsigned long)g_stat.tcpDisconnects,
          (unsigned long)g_stat.authFailures, (unsigned long)g_stat.authLockouts,
          (unsigned long)g_stat.httpWriteFail,
          (unsigned long)g_stat.cfgSaveFail, (unsigned long)g_stat.cfgCrcFail,
          (unsigned long)g_stat.tcpAccepted, (unsigned long)g_stat.tcpEvicted, (unsigned long)g_stat.tcpDropped,
          (unsigned long)g_stat.tcpRejected, (unsigned long)g_stat.internalErr,
          (unsigned long)g_stat.ethRecoveries,
          (unsigned long)esp_get_free_heap_size(),
          (unsigned long)(g_netTask ? uxTaskGetStackHighWaterMark(g_netTask) : 0),
          (unsigned long)(g_rtuTask ? uxTaskGetStackHighWaterMark(g_rtuTask) : 0),
          (unsigned long)(g_turnaroundLastUs / 1000), (unsigned long)(g_turnaroundMaxUs / 1000));

  bool anyUnit = false;
  for (const auto& u : g_unitStat) if (u.used) { anyUnit = true; break; }
  if (anyUnit) {
    pageRaw("<div class=\"st\">Per Slave (unit id)</div><table>"
            "<tr><td><b>Unit</b></td><td><b>OK</b></td><td><b>Failed</b></td></tr>");
    for (const auto& u : g_unitStat)
      if (u.used)
        pageFmt("<tr><td>%u</td><td>%lu</td><td>%lu</td></tr>",
                u.unit, (unsigned long)u.ok, (unsigned long)u.fail);
    pageRaw("</table>");
  }
  pageFmt("<p style='font-size:11px;color:#7f8c8d'>"
          "Counters live in RAM only and reset on reboot. Capacity: %u TCP masters, "
          "%u pipelined requests each, one RS-485 transaction at a time. "
          "Serial: Modbus RTU only (no ASCII), 1200-115200 baud, 8 data bits, "
          "N/E/O parity, 1-2 stop bits.</p>",
          (unsigned)MAX_CLIENTS, (unsigned)REQ_QUEUE_DEPTH)
          ;
  pageRaw("<p style='font-size:11px;color:#7f8c8d'><a href=\"/config.json\">Export configuration"
          "</a></p></div></body></html>");

  if (g_pageOverflow) {
    LOGF("[HTTP] page needs %u bytes, buffer is %u -> raise PAGE_BUFFER_SIZE\n",
         (unsigned)g_pageNeeded, (unsigned)PAGE_BUFFER_SIZE);
    char msg[120];
    snprintf(msg, sizeof(msg),
             "Page needs %u bytes but the build buffer is %u. Raise PAGE_BUFFER_SIZE.",
             (unsigned)g_pageNeeded, (unsigned)PAGE_BUFFER_SIZE);
    httpSendSimple(c, 500, "Internal Error", msg);
    return;
  }
  LOGF("[HTTP] config page %u bytes (buffer %u)\n",
       (unsigned)g_pageLen, (unsigned)PAGE_BUFFER_SIZE);
  httpSend(c, 200, "OK", "text/html", g_page, g_pageLen);
}

static void handleSave(EthernetClient& c, const char* body) {
  char f_ip[20], f_sn[20], f_gw[20], f_port[8], f_baud[8], f_fmt[4], f_to[8], f_csrf[24], f_exc[4];
  IPAddress ip, sn, gw;
  uint32_t port = 0, baud = 0, fmt = 0, to = 0, exc = 1;

  // Same-origin proof: the token was handed out with the form (audit FIX-15).
  if (!formField(body, "csrf", f_csrf, sizeof(f_csrf)) || !csrfValid(f_csrf)) {
    httpSendSimple(c, 403, "Forbidden", "Stale or missing form token. Reload the page and try again.");
    return;
  }

  bool ok = formField(body, "l_ip", f_ip, sizeof(f_ip)) && parseIPv4(f_ip, ip) &&
            formField(body, "l_sn", f_sn, sizeof(f_sn)) && parseIPv4(f_sn, sn) &&
            formField(body, "l_gw", f_gw, sizeof(f_gw)) && parseIPv4(f_gw, gw) &&
            formField(body, "m_port", f_port, sizeof(f_port)) && parseU32(f_port, 1, 65535, port) &&
            formField(body, "baud", f_baud, sizeof(f_baud)) && parseU32(f_baud, 1, 1000000, baud) &&
            formField(body, "fmt", f_fmt, sizeof(f_fmt)) && parseU32(f_fmt, 0, SERIAL_FMT_COUNT - 1, fmt) &&
            formField(body, "rtu_to", f_to, sizeof(f_to)) && parseU32(f_to, 20, 5000, to) &&
            formField(body, "exc", f_exc, sizeof(f_exc)) && parseU32(f_exc, 0, 1, exc);
  ok = ok && networkValid(ip, sn, gw) && portValid(port) && baudValid(baud);

  if (!ok) {
    httpSendSimple(c, 400, "Bad Request",
                   "Invalid input. Check IP / subnet / gateway (same subnet), port (1-65535, not 80), baud and timeout.");
    return;
  }

  // Optional password change, validated before anything is written.
  char pw1[WEB_PASS_MAX_LEN + 2] = {0}, pw2[WEB_PASS_MAX_LEN + 2] = {0};
  bool havePw1 = formField(body, "pw1", pw1, sizeof(pw1));
  bool havePw2 = formField(body, "pw2", pw2, sizeof(pw2));
  bool changePw = (havePw1 && pw1[0]) || (havePw2 && pw2[0]);
  if (changePw) {
    size_t l1 = strlen(pw1);
    if (strcmp(pw1, pw2) != 0 || l1 < WEB_PASS_MIN_LEN || l1 > WEB_PASS_MAX_LEN) {
      memset(pw1, 0, sizeof(pw1)); memset(pw2, 0, sizeof(pw2));
      httpSendSimple(c, 400, "Bad Request",
                     "The two passwords must match and be 8-32 characters long.");
      return;
    }
  }

  // One atomic blob: a power cut can no longer mix old and new values (FIX-04).
  GatewayConfig next;
  next.ip = ip; next.sn = sn; next.gw = gw;
  next.port = (uint16_t)port; next.baud = baud;
  next.fmt = (uint8_t)fmt; next.rtuTimeoutMs = (uint16_t)to;
  next.flags = exc ? CFG_FLAG_TCP_EXCEPTION : 0;

  // Settings and credentials are ONE record, so this is a single verified write
  // to the inactive slot: power loss here leaves the old slot fully intact and
  // the new one either valid or ignored (audit FIX-A2).
  uint8_t newHash[32];
  bool newIsDefault = g_pwIsDefault;
  memcpy(newHash, g_pwHash, sizeof(newHash));
  if (changePw) {
    hashCredentials(WEB_AUTH_USER, pw1, newHash);
    newIsDefault = false;
  }
  memset(pw1, 0, sizeof(pw1));
  memset(pw2, 0, sizeof(pw2));

  if (!persistConfig(next, newHash, newIsDefault)) {
    g_stat.cfgSaveFail++;
    httpSendSimple(c, 500, "Internal Error",
                   "Nothing was changed: the settings could not be stored.");
    return;
  }
  memcpy(g_pwHash, newHash, sizeof(g_pwHash));
  g_pwIsDefault = newIsDefault;
  // The token was consumed by csrfValid(); other open tabs keep their own.

  char s_ip[16];
  snprintf(s_ip, sizeof(s_ip), "%u.%u.%u.%u", ip[0], ip[1], ip[2], ip[3]);
  pageReset();
  pageFmt("<!DOCTYPE html><html><body style='font-family:Arial;text-align:center;padding:50px;'>"
          "<h2 style='color:#2ecc71;'>Settings Saved!</h2><p>Device is restarting...</p>"
          "<p>New address: http://%s/</p>%s</body></html>",
          s_ip, changePw ? "<p>The new password is active after the restart.</p>" : "");
  httpSend(c, 200, "OK", "text/html", g_page, g_pageLen);
  g_restartPending = true;                          // restart from the task loop, not here
  g_restartAtMs = millis() + 800;
}

static void handleHttpRequest(EthernetClient& c, char* req, size_t hdrLen, const char* body) {
  // Request line: METHOD SP PATH SP VERSION
  char method[8] = {0}, path[64] = {0};
  const char* sp1 = strchr(req, ' ');
  const char* sp2 = sp1 ? strchr(sp1 + 1, ' ') : nullptr;
  if (!sp1 || !sp2 || (size_t)(sp1 - req) >= sizeof(method) || (size_t)(sp2 - sp1 - 1) >= sizeof(path)) {
    httpSendSimple(c, 400, "Bad Request", "Malformed request line.");
    return;
  }
  memcpy(method, req, (size_t)(sp1 - req));
  memcpy(path, sp1 + 1, (size_t)(sp2 - sp1 - 1));
  char* q = strchr(path, '?'); if (q) *q = '\0';

  // HEAD is answered like GET but without a body (audit FIX-09).
  const bool isHead = (strcmp(method, "HEAD") == 0);
  g_httpHeadOnly = isHead;

  const uint32_t now = millis();
  AuthHost* host = authSlot(ipToU32(c.remoteIP()));
  if (host->lockUntil && (int32_t)(now - host->lockUntil) < 0) {
    httpSendSimple(c, 429, "Too Many Requests", "Too many failed logins from this host. Try again later.");
    return;
  }

  if (now - g_authWindowStart > AUTH_GLOBAL_WINDOW_MS) {
    g_authWindowStart = now;
    g_authFailWindow = 0;
  }
  if (g_authFailWindow > AUTH_GLOBAL_MAX_FAILS) {   // brake, whatever the source IP
    httpSendSimple(c, 429, "Too Many Requests", "Too many failed logins. Try again later.");
    return;
  }

  bool credentialsPresent = false;
  if (!httpAuthorized(req, hdrLen, credentialsPresent)) {
    if (credentialsPresent) {
      g_authFailWindow++;
      g_stat.authFailures++;
      if (++host->fails >= AUTH_MAX_FAILS) {
        host->fails = 0;
        host->lockUntil = now + AUTH_LOCKOUT_MS;
        g_stat.authLockouts++;
        LOGF("[SEC] login lockout for %u.%u.%u.%u\n",
             c.remoteIP()[0], c.remoteIP()[1], c.remoteIP()[2], c.remoteIP()[3]);
      }
    }
    httpSendSimple(c, 401, "Unauthorized", "Login required.",
                   "WWW-Authenticate: Basic realm=\"Modbus Gateway\"\r\n");
    return;
  }
  host->fails = 0;
  host->lockUntil = 0;

  if ((strcmp(method, "GET") == 0 || isHead) && strcmp(path, "/") == 0) {
    sendConfigPage(c);
    g_httpHeadOnly = false;
    return;
  }

  // Configuration export (authenticated). Import is done through the form; the
  // JSON is for site documentation and for cloning settings by hand (FIX-A15).
  if ((strcmp(method, "GET") == 0 || isHead) && strcmp(path, "/config.json") == 0) {
    pageReset();
    pageFmt("{\"fw\":\"%s\",\"mac\":\"%02X:%02X:%02X:%02X:%02X:%02X\","
            "\"ip\":\"%u.%u.%u.%u\",\"mask\":\"%u.%u.%u.%u\",\"gw\":\"%u.%u.%u.%u\","
            "\"modbus_port\":%u,\"baud\":%lu,\"format\":\"%s\",\"rtu_timeout_ms\":%u,"
            "\"tcp_exception\":%s,\"max_clients\":%u,\"queue_depth\":%u}",
            FW_VERSION, mac[0], mac[1], mac[2], mac[3], mac[4], mac[5],
            cfg.ip[0], cfg.ip[1], cfg.ip[2], cfg.ip[3],
            cfg.sn[0], cfg.sn[1], cfg.sn[2], cfg.sn[3],
            cfg.gw[0], cfg.gw[1], cfg.gw[2], cfg.gw[3],
            cfg.port, (unsigned long)cfg.baud, SERIAL_FMTS[cfg.fmt].name, cfg.rtuTimeoutMs,
            (cfg.flags & CFG_FLAG_TCP_EXCEPTION) ? "true" : "false", (unsigned)MAX_CLIENTS,
            (unsigned)REQ_QUEUE_DEPTH);
    httpSend(c, 200, "OK", "application/json", g_page, g_pageLen);
    g_httpHeadOnly = false;
    return;
  }

  if (strcmp(method, "POST") == 0 && strcmp(path, "/save") == 0) {
    // Be explicit about what this tiny server does and does not accept
    // (audit FIX-15) instead of silently treating it as an empty body.
    char v[48];
    if (httpHeader(req, hdrLen, "Transfer-Encoding", v, sizeof(v))) {
      httpSendSimple(c, 501, "Not Implemented", "Chunked request bodies are not supported.");
      return;
    }
    if (!httpHeader(req, hdrLen, "Content-Length", v, sizeof(v))) {
      httpSendSimple(c, 411, "Length Required", "A Content-Length header is required.");
      return;
    }
    if (!httpHeader(req, hdrLen, "Content-Type", v, sizeof(v)) ||
        strncasecmp(v, "application/x-www-form-urlencoded", 33) != 0) {
      httpSendSimple(c, 415, "Unsupported Media Type", "Expected application/x-www-form-urlencoded.");
      return;
    }
    handleSave(c, body);
    return;
  }

  if (strcmp(method, "GET") == 0 || isHead || strcmp(method, "POST") == 0)
    httpSendSimple(c, 404, "Not Found", "No such page.");
  else
    httpSendSimple(c, 405, "Method Not Allowed", "Only GET, HEAD and POST are supported.");
  g_httpHeadOnly = false;
}

static void acceptHttpClients() {
  for (int guard = 0; guard < MAX_SOCK_NUM; guard++) {
    EthernetClient nc = httpServer.accept();
    if (!nc) return;
    forgetStaleHolders(nc.getSocketNumber());
    nc.setConnectionTimeout(SOCKET_CLOSE_TIMEOUT_MS);
    if (g_http.active && g_http.len > 0) {          // busy with a real request
      nc.stop();
      continue;
    }
    if (g_http.active) closeHttpSession();          // idle/pre-connect socket: replace
    g_http.sock = nc;
    g_http.active = true;
    g_http.len = 0;
    g_http.startMs = millis();
  }
}

static void serviceHttp() {
  if (!g_http.active) return;
  EthernetClient& c = g_http.sock;
  if (!c.connected()) { closeHttpSession(); return; }

  int avail = c.available();
  while (avail > 0 && g_http.len < HTTP_BUF_SIZE - 1) {
    size_t room = HTTP_BUF_SIZE - 1 - g_http.len;
    int n = c.read((uint8_t*)g_http.buf + g_http.len, (size_t)avail < room ? (size_t)avail : room);
    if (n <= 0) break;
    g_http.len += (uint16_t)n;
    avail = c.available();
  }
  g_http.buf[g_http.len] = '\0';

  char* hdrEnd = strstr(g_http.buf, "\r\n\r\n");
  if (hdrEnd) {
    size_t hdrLen = (size_t)(hdrEnd - g_http.buf) + 4;
    char clBuf[12];
    uint32_t contentLen = 0;
    if (httpHeader(g_http.buf, hdrLen, "Content-Length", clBuf, sizeof(clBuf)) &&
        !parseU32(clBuf, 0, HTTP_BUF_SIZE, contentLen)) {
      httpSendSimple(c, 400, "Bad Request", "Bad Content-Length.");
      closeHttpSession(); return;
    }
    if (hdrLen + contentLen >= HTTP_BUF_SIZE) {
      httpSendSimple(c, 413, "Payload Too Large", "Request too large.");
      closeHttpSession(); return;
    }
    if (g_http.len >= hdrLen + contentLen) {
      g_http.buf[hdrLen + contentLen] = '\0';
      handleHttpRequest(c, g_http.buf, hdrLen, g_http.buf + hdrLen);
      closeHttpSession();
      return;
    }
  } else if (g_http.len >= HTTP_BUF_SIZE - 1) {
    httpSendSimple(c, 431, "Request Header Fields Too Large", "Headers too large.");
    closeHttpSession(); return;
  }

  if (millis() - g_http.startMs > HTTP_REQUEST_TIMEOUT_MS) closeHttpSession();
}

// ==========================================================================
// 12. NETWORK TASK — sole owner of W5500 / SPI / Ethernet library
// ==========================================================================
static void netTask(void* arg) {
  esp_err_t e = esp_task_wdt_add(nullptr);
  g_twdtSubscribed = (e == ESP_OK);
  if (!g_twdtSubscribed) LOGF("[WDT] task WDT unavailable (%d) - relying on external WDT\n", (int)e);

  uint32_t lastLink = 0, lastHealth = millis();
  for (;;) {
    netAlive();
    uint32_t now = millis();

    if (g_restartPending && (int32_t)(now - g_restartAtMs) >= 0) {
      for (auto& s : g_mb) if (s.active) closeMbSession(s);
      if (g_http.active) closeHttpSession();
      LOGF("[SYS] restarting to apply new configuration\n");
      Serial.flush();
      esp_restart();
    }

    if (now - lastLink >= LINK_POLL_MS) {
      lastLink = now;
      g_linkUp = g_ethReady && (Ethernet.linkStatus() == LinkON);
    }
    if (now - lastHealth >= HEALTH_CHECK_MS) {
      lastHealth = now;
      // Three consecutive bad reads before acting: one glitched SPI transfer
      // must not tear down healthy TCP sessions (audit FIX-13).
      if (!g_ethReady || !ethernetHealthy()) {
        if (++g_ethStrikes >= ETH_HEALTH_STRIKES) {
          g_ethStrikes = 0;
          ethernetRecover();
          g_ethOk = g_ethReady;                     // fault LED only after a real recovery
        }
      } else {
        g_ethStrikes = 0;
        g_ethOk = g_ethReady;
      }
    }

    if (g_ethReady) {
      // Order matters: all accept() calls (which may recycle sockets) run
      // before any session reads, so a stale slot can never read a new
      // connection's data.
      completeRtuJob();          // reply to SCADA if RtuTask finished
      acceptModbusClients();
      acceptHttpClients();
      serviceModbusSessions();
      dispatchRtuJob();          // start next RS-485 transaction (non-blocking)
      serviceHttp();
    }
    vTaskDelay(1);
  }
}

// ==========================================================================
// 13. LEDs + EXTERNAL WATCHDOG SUPERVISOR (loop task, no SPI access)
// ==========================================================================
static void handleLEDs() {
  static int8_t lastD3 = -1, lastD4 = -1;
  uint32_t traffic = g_lastTrafficMs;
  uint32_t now = millis();
  int d3, d4;
  if (g_linkUp) {
    d4 = HIGH;                                              // link: steady ON
    d3 = (now - traffic < 80) ? HIGH : LOW;                 // Modbus activity flash
  } else if (!g_ethOk) {
    bool blink = (now % 400) < 200;                         // W5500 fault: ALTERNATING
    d3 = blink ? HIGH : LOW;
    d4 = blink ? LOW : HIGH;
  } else {
    bool blink = (now % 400) < 200;                         // cable unplugged: both together
    d3 = d4 = blink ? HIGH : LOW;
  }
  if (d3 != lastD3) { digitalWrite(LED_D3, d3); lastD3 = (int8_t)d3; }
  if (d4 != lastD4) { digitalWrite(LED_D4, d4); lastD4 = (int8_t)d4; }
}

static void superviseExternalWatchdog() {
  static uint32_t lastFeed = 0;
  uint32_t hbNet = g_netHeartbeatMs;                        // read BEFORE millis()
  uint32_t hbRtu = g_rtuHeartbeatMs;
  uint32_t now = millis();
  if (now - hbNet > NET_HEARTBEAT_MAX_AGE_MS) return;       // NetTask hung: let ext. WDT reset us
  // A legitimate transaction (bus-free wait + TX + response timeout) must fit
  // inside this window, or a slow slave would look like a hung task (FIX-04).
  if (now - hbRtu > NET_HEARTBEAT_MAX_AGE_MS + cfg.rtuTimeoutMs + 1000UL) return;
  if (now - lastFeed >= EXT_WDT_FEED_MS) { lastFeed = now; pulseExternalWatchdog(); }
}

// ==========================================================================
// 14. SETUP / LOOP
// ==========================================================================
void setup() {
  Serial.begin(115200);

  pinMode(LED_D3, OUTPUT);
  pinMode(LED_D4, OUTPUT);
  pinMode(WDT_DONE_PIN, OUTPUT);
  digitalWrite(WDT_DONE_PIN, LOW);
  digitalWrite(LED_D3, LOW);
  digitalWrite(LED_D4, LOW);
  pulseExternalWatchdog();

#if (RS485_DE_PIN >= 0) && !RS485_USE_HW_DE
  pinMode(RS485_DE_PIN, OUTPUT);
  digitalWrite(RS485_DE_PIN, LOW);                          // receive by default
#endif

  // ORDER IS LOAD-BEARING (audit FIX-01): the per-device default password is
  // derived from the MAC, and loadConfig() may have to derive/store it (first
  // boot, migration, corrupted record). The MAC must therefore exist before any
  // persistence code runs. Nothing above this line may touch credentials.
  generateUniqueMac(mac);

  if (!prefs.begin("modbus_cfg", false)) LOGF("[CFG] NVS unavailable -> running on defaults\n");
  factoryResetCheck();
  loadConfig();
  computeRtuTiming();
  authReport();

  rs485Init();

#if W5500_RST >= 0
  pinMode(W5500_RST, OUTPUT);
  digitalWrite(W5500_RST, W5500_RST_RELEASE_LEVEL);   // do NOT hold the chip in reset
#endif
  // SS = -1: the Ethernet library drives CS by hand, so the ESP32 must not also
  // route a hardware CS signal onto the same pin (audit FIX-58).
  SPI.begin(W5500_SCLK, W5500_MISO, W5500_MOSI, -1);
  pinMode(W5500_CS, OUTPUT);
  digitalWrite(W5500_CS, HIGH);

  // Apply the configured port to the STATIC server object (no heap).
  modbusServer = CustomEthernetServer(cfg.port);

  pulseExternalWatchdog();
  g_ethReady = ethernetInit();                              // retried by NetTask if it fails
  pulseExternalWatchdog();

  LOGF("\n=== Ajeevi Modbus Gateway v%s ===\n", FW_VERSION);
  LOGF("Last reset: %s\n", resetReasonName());
  LOGF("MAC  %02X:%02X:%02X:%02X:%02X:%02X\n", mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
  LOGF("IP   %u.%u.%u.%u  mask %u.%u.%u.%u  gw %u.%u.%u.%u\n",
       cfg.ip[0], cfg.ip[1], cfg.ip[2], cfg.ip[3], cfg.sn[0], cfg.sn[1], cfg.sn[2], cfg.sn[3],
       cfg.gw[0], cfg.gw[1], cfg.gw[2], cfg.gw[3]);
  LOGF("Modbus TCP :%u  HTTP :%u  RTU %lu %s timeout %u ms  W5500 %s\n",
       cfg.port, HTTP_PORT, (unsigned long)cfg.baud, SERIAL_FMTS[cfg.fmt].name, cfg.rtuTimeoutMs,
       g_ethReady ? "OK" : "NOT FOUND");
  LOGF("Web login user '%s', password %s\n", WEB_AUTH_USER,
       g_pwIsDefault ? "= built-in default (change it!)" : "set by operator");

  // From here on ONLY NetTask touches the W5500. Same core as loop() and
  // same priority, so the Ethernet library's yield()-based busy waits
  // round-robin with loop() instead of starving it.
  g_rtuJobQ  = xQueueCreate(1, sizeof(uint32_t));
  g_rtuDoneQ = xQueueCreate(1, sizeof(uint32_t));
  if (!g_rtuJobQ || !g_rtuDoneQ) { LOGF("[SYS] queue alloc failed -> restart\n"); delay(100); esp_restart(); }
  g_ethOk = g_ethReady;

  // RtuTask owns ONLY the UART (never the W5500), so it can run on the other
  // core and block on the serial line without stalling the network.
  g_rtuHeartbeatMs = millis();
  BaseType_t okRtu = xTaskCreatePinnedToCore(rtuTask, "RtuTask", RTU_TASK_STACK, nullptr,
                                             RTU_TASK_PRIORITY, &g_rtuTask,
                                             xPortGetCoreID() == 0 ? 1 : 0);

  g_netHeartbeatMs = millis();
  BaseType_t okNet = xTaskCreatePinnedToCore(netTask, "NetTask", NET_TASK_STACK, nullptr,
                                             uxTaskPriorityGet(nullptr), &g_netTask,
                                             xPortGetCoreID());
  if (okRtu != pdPASS || okNet != pdPASS) {         // nothing works without both (audit FIX-17)
    LOGF("[SYS] task creation failed (rtu=%d net=%d) -> restart\n", (int)okRtu, (int)okNet);
    Serial.flush();
    delay(100);
    esp_restart();
  }
}

void loop() {
  handleLEDs();
  superviseExternalWatchdog();
  vTaskDelay(pdMS_TO_TICKS(20));
}
