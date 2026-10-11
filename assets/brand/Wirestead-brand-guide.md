# wirestead visual identity — square wrapper mark

## Core concept

The mark represents the **software abstraction**, not a physical cable, network
switch, or protocol bridge. Three differently colored transport paths converge
into a single consistent interface **inside stable brackets**. The brackets
express a wrapper: diverse Serial, TCP, UDP, and UDS transports share a reliable
C++20 API and predictable runtime behavior.

The logo deliberately uses a balanced **1:1 square mark** rather than a wide
wiring diagram. Brackets, smooth convergence, circular inputs, and the
blue/cyan/teal progression are integral to this identity; avoid replacing them
with arrowheads, PCB traces, or sharp merge corners.

Brand attributes: **unified, asynchronous, reliable, cross-platform**.

## Naming and visual wordmark

| Context | Use |
| --- | --- |
| Brand in prose, docs, release titles | `Wirestead` |
| Visual wordmark (logo artwork) | **`wirestead`** (lowercase) |
| C++ namespace, packages, GitHub | `wirestead` |
| CMake target | `wirestead::wirestead` |

The visual wordmark is font-independent vector artwork (outlines).
Keep the brand name capitalized in ordinary prose and source code identifiers
unchanged; this change is visual only.

## Color palette

| Role | Color | Hex |
| --- | --- | --- |
| Stable brackets / wordmark | Ink navy | `#0F172A` |
| Upper path | Blue | `#2563EB` |
| Middle path | Cyan | `#06B6D4` |
| Lower path | Teal | `#10B981` |
| Light background | White | `#FFFFFF` |

Dark background logo uses white brackets and a white wordmark with brighter path
colors for contrast.

## Production assets

- `assets/wirestead-mark.svg` — 1:1 transparent mark (square underlying symbol).
- `assets/wirestead-mark-monochrome.svg` — 1:1 single-color mark.
- `assets/wirestead-favicon.svg` — square dark badge for app/favicon use.
- `assets/wirestead-icon-dark.svg` / `assets/wirestead-icon-light.svg` — square avatars.
- `assets/wirestead-logo-square.svg` — square stacked mark + lowercase wordmark.
- `assets/wirestead-logo-horizontal.svg` — README / document lockup.
- `assets/wirestead-logo-dark.svg` — dark-background lockup.
- `assets/wirestead-logo-monochrome.svg` — monochrome lockup.

All SVGs use vector outlines for the symbol and wordmark, with no external
font or linked resource dependency. Matching PNGs are provided for apps that
cannot consume SVG. Re-export PNGs from their SVG counterparts after a vector
change; do not hand-edit raster assets.

## Usage constraints

- Retain square aspect ratio for standalone badges and icons. Do not stretch
  the symbol or add compensating horizontal padding to disguise distortion.
- Prefer 64 px or greater for full-color avatar display; at small sizes, test
  the three paths independently. Use the monochrome mark when contrast is low.
- Keep at least 1/8 of the mark's side as surrounding clear space.
- Do not invert the path order, add arrowheads or shadows, or recolor
  individual strands arbitrarily.

## Positioning

Primary message: **Simple, reliable async communication.**

Expanded: **One consistent C++20 interface for asynchronous communication
across Serial, TCP, UDP, and UDS.**
