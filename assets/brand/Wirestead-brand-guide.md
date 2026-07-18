# Wirestead visual identity — revised direction

## Core idea

The symbol uses two bold, offset arrows: the blue path sends while the green path receives. Their different vertical positions express asynchronous progress without a shared clock or blocking loop. The two forms interlock into one compact mark, reflecting Wirestead's unified interface across Serial, TCP, UDP, and UDS.

Brand attributes: **simple, reliable, asynchronous, unified**.

## Name system

| Context | Use |
| --- | --- |
| Brand, documentation, release titles | `Wirestead` |
| Visual wordmark | `wirestead` |
| GitHub organization/repository | `wirestead/wirestead` |
| vcpkg, Conan, pkg-config | `wirestead` |
| CMake target | `wirestead::wirestead` |
| C++ namespace and headers | `wirestead`, `<wirestead/...>` |
| Macros and environment variables | `WIRESTEAD_*` |

Use `Wirestead` in running text and the supplied lowercase artwork for the visual wordmark. Do not use `WireStead`, `wireStead`, or spaced `Wire Stead`.

## Palette

| Role | Name | Hex |
| --- | --- | --- |
| Primary text and dark surfaces | Core Ink | `#071426` |
| Outbound flow | Send Blue | `#147AE6` |
| Inbound flow | Receive Green | `#00C98B` |
| Light surface | Open White | `#FFFFFF` |
| Secondary text | Slate | `#66758B` |

Dark-background accents use `#46A6FF` and `#20D9A2` for stronger contrast.

## Typography

- Recommended product typeface: **Inter**, 600–700 for headings and 400–500 for body copy.
- Recommended technical typeface: **JetBrains Mono** or **IBM Plex Mono**.
- The supplied production SVG logos convert the wordmark to paths and are font-independent. Editable sources use Nimbus Sans as a portable fallback.

## Clear space and minimum size

- Clear space: keep at least one quarter of the symbol width around the mark.
- Symbol minimum: 16 px digital, 6 mm print.
- Horizontal lockup minimum: 120 px digital, 32 mm print.
- Below the minimum, use the symbol without the wordmark.

## Usage

- Use the primary logo on white or very light neutral backgrounds.
- Use the dark variant on Core Ink or similarly dark backgrounds.
- Use the monochrome variant for engraving, terminal-like contexts, single-color print, and sponsor grids.
- Do not reverse the arrow directions, change the relative offset, add outlines or shadows, or place the mark on visually noisy backgrounds.

## Message line

Primary English line: **Simple, reliable async communication.**

Expanded positioning: **A simple C++20 interface for reliable asynchronous communication across Serial, TCP, UDP, and UDS.**

The message line is not part of the registered wordmark and should remain removable from layouts.
