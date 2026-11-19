---
name: DistributedFileSync
description: "Mocha-themed JavaFX desktop interface for the DACS4 file synchronization demo."
colors:
  primary: "#89b4fa"
  primary-hover: "#a7c5fb"
  mauve: "#cba6f7"
  base: "#1e1e2e"
  mantle: "#181825"
  surface: "#313244"
  surface-hover: "#45475a"
  text: "#cdd6f4"
  muted: "#a6adc8"
  nav-selected: "#30324b"
  row-hover: "#292b40"
  row-selected: "#3b4160"
  success: "#a6e3a1"
  success-background: "#273b37"
  warning: "#f9e2af"
  warning-background: "#40382f"
  danger: "#f38ba8"
  danger-background: "#43303e"
  info-background: "#29354e"
typography:
  headline: {fontFamily: "Segoe UI", fontSize: "25px", fontWeight: 700}
  metric: {fontFamily: "Segoe UI", fontSize: "24px", fontWeight: 700}
  title: {fontFamily: "Segoe UI", fontSize: "16px", fontWeight: 700}
  brand: {fontFamily: "Segoe UI", fontSize: "14px", fontWeight: 700}
  dialog-title: {fontFamily: "Segoe UI", fontSize: "15px", fontWeight: 700}
  body: {fontFamily: "Segoe UI", fontSize: "13px"}
  badge: {fontFamily: "Segoe UI", fontSize: "12px", fontWeight: 700}
  label: {fontFamily: "Segoe UI", fontSize: "10px", fontWeight: 700}
rounded:
  control: "8px"
  panel: "10px"
  card: "12px"
spacing:
  action-gap: "8px"
  field-gap: "10px"
  card-gap: "12px"
  card-inset: "14px"
  section-gap: "16px"
  page-gutter: "24px"
components:
  button-primary:
    backgroundColor: "{colors.primary}"
    textColor: "{colors.mantle}"
    rounded: "{rounded.control}"
    padding: "6px 12px"
  button-primary-hover:
    backgroundColor: "{colors.primary-hover}"
  button-secondary:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.text}"
    rounded: "{rounded.control}"
    padding: "6px 12px"
  button-secondary-hover:
    backgroundColor: "{colors.surface-hover}"
  text-field:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.text}"
    rounded: "{rounded.control}"
    padding: "6px 10px"
  nav-item:
    backgroundColor: "transparent"
    textColor: "{colors.text}"
    rounded: "{rounded.control}"
    padding: "10px"
  nav-active:
    backgroundColor: "{colors.nav-selected}"
    textColor: "{colors.primary}"
  card:
    backgroundColor: "{colors.surface}"
    rounded: "{rounded.card}"
    padding: "14px"
  badge-success:
    backgroundColor: "{colors.success-background}"
    textColor: "{colors.success}"
    typography: "{typography.badge}"
    rounded: "{rounded.card}"
    padding: "3px 9px"
  table:
    backgroundColor: "{colors.mantle}"
    textColor: "{colors.text}"
    rounded: "{rounded.panel}"
    padding: "4px"
---

# Design System — DistributedFileSync

## Overview

The interface uses a compact desktop console layout. A persistent sidebar holds the DFS mark, five destinations, and a demo notice. The workspace pairs a prominent Vietnamese page title with a connection badge, then uses compact controls, flat cards, and dense tables.

- Mocha surfaces distinguish the shell, workspace, and grouped content.
- Pastel colors identify actions, focus, and status.
- Outline icons accompany navigation labels.

The implementation lives in `@src/main/resources/com/dacs4/filesync/theme/mocha.css` and `@src/main/resources/com/dacs4/filesync/view/shell.fxml`.

## Colors

- **Primary.** Soft blue marks primary actions, focus borders, the active navigation label and icon, and informational text. Primary buttons use dark text and a lighter blue hover fill.
- **Brand accent.** Mauve is used on the DFS mark and uppercase sidebar labels.
- **Status.** Green signals synced or online, yellow signals pending, and rose signals conflict. Blue marks information such as a version or timestamp. Gray marks offline. Badges pair colored text with tinted fills.
- **Neutrals.** Mantle anchors the sidebar, header, and table frame. Base fills the page canvas. Surface fills cards, controls, and column headers. Main text is pale lavender, with a muted tone for supporting labels and placeholders.

## Typography

Segoe UI is the shared family. The bold page title (25px) and metric values (24px) sit above section headings (16px), dialog headings (15px), and the sidebar brand name (14px). Body copy and table cells use the base size (13px). Status badges use bold type (12px), and uppercase sidebar labels use bold type (10px).

## Layout

The desktop stage opens at 1280 × 800 and has a 1050 × 680 minimum. A 224px preferred-width sidebar sits beside a growing workspace. The header and page share a 24px horizontal gutter. Pages use 16px vertical section gaps, with 12px between cards and 8–10px between controls.

The dashboard metric cards and conflict comparison cards share available width evenly. The tables grow vertically and scroll within their own bounds. The file search field expands ahead of the status selector. The sidebar and side-by-side groups remain in place at the minimum window size.

## Elevation & Depth

The theme uses flat fills. The header edge and table frame use surface-colored borders. Alternating rows distinguish records, with separate fills for hover and selection.

## Shapes

Controls use 8px corners. The brand mark, sidebar notice, and table frame use 10px corners. Cards and badges use 12px corners. Table headers round the top corners by 6px.

## Components

- **Buttons.** Primary buttons use bold dark text over blue. Secondary buttons use the neutral surface and a brighter hover fill. Both have a 34px minimum height and a blue focus border.
- **Fields and selectors.** Use the same minimum height and radius as buttons. Text fields have muted prompts. Selectors use a lighter hover fill and a surface-colored popup. Both show the blue focus border. Server connection fields use an inset fill with an outline and show Vietnamese validation messages below the corresponding field.
- **Sidebar navigation.** Full-width, left-aligned buttons pair outline icons with text. Items rest on the sidebar background, pick up the surface fill on hover, and use a tinted fill with bold blue text and icon when active.
- **Cards.** Use the surface fill and 14px padding. Dashboard metric cards pair a muted label with a bold count. Conflict cards align local and server metadata in matching label/value rows.
- **Status badges.** Use short Vietnamese labels for states such as “Đã đồng bộ”, “Đang chờ”, and “Xung đột”. The header uses the informational blue treatment for “Kết nối mô phỏng”.
- **Tables.** Use muted column labels, 36px headers, and 38px rows. File and version sizes align to the right. Rows alternate mantle and base, with distinct hover and selection fills. Empty tables use a text placeholder.
- **Dialogs.** Use a base-colored pane, a rounded surface header, wrapped copy, and Vietnamese action labels. Confirmation dialogs offer a cancel action.

## Do's and Don'ts

- Do pair semantic colors with written status labels.
- Do keep the active navigation fill and keyboard focus border visually distinct.
- Do align comparison cards so the same fields can be scanned across both versions.
