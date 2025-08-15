"use client";

import { motion } from "framer-motion";

const palette = {
  blue: "#0098d8",
  red: "#f54123",
  paper: "#e5e7de",
  ink: "#0b3536",
} as const;

export default function Page() {
  return (
    <main className="min-h-dvh grid grid-rows-[auto_1fr_auto] p-6 md:p-10">
      {/* Masthead */}
      <header className="flex items-baseline justify-between">
        <motion.h1
          initial={{ y: 8, opacity: 0 }}
          animate={{ y: 0, opacity: 1 }}
          transition={{ duration: 0.45, ease: [0.2, 0.8, 0.2, 1] }}
          className="font-medium tracking-tight text-[21px]"
        >
          FIND FUNDING
        </motion.h1>
        <motion.span
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.2, duration: 0.6 }}
          className="text-[11px] uppercase tracking-[0.18em]"
          style={{ color: palette.blue }}
        >
          field manual / 1960
        </motion.span>
      </header>

      {/* Body */}
      <section className="flex items-center">
        <div className="max-w-[58ch]">
          <motion.p
            initial={{ opacity: 0, y: 4 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.05, duration: 0.5 }}
            className="text-[28px] leading-tight md:text-[32px] md:leading-tight font-normal"
          >
            A minimal index of philanthropic filings and grants. Designed with
            restraint and legibility.
          </motion.p>

          <div className="mt-8 grid gap-3">
            <NavItem label="Index" kbd="I" color={palette.blue} />
            <NavItem label="Filers" kbd="F" color={palette.ink} />
            <NavItem label="Grants" kbd="G" color={palette.red} />
          </div>
        </div>
      </section>

      {/* Footer */}
      <footer
        className="flex items-center justify-between pt-6 border-t"
        style={{ borderColor: "#0b3536" }}
      >
        <small className="text-[11px] tracking-wide">
          © {new Date().getFullYear()} Project Donors
        </small>
        <div className="flex gap-3">
          <Legend swatch={palette.blue} label="link" />
          <Legend swatch={palette.red} label="action" />
          <Legend swatch={palette.ink} label="text" />
        </div>
      </footer>
    </main>
  );
}

function NavItem({
  label,
  kbd,
  color,
}: {
  label: string;
  kbd: string;
  color: string;
}) {
  return (
    <motion.a
      href="#"
      initial={{ opacity: 0, y: 2 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4 }}
      className="group flex items-center justify-between border px-3 py-2 select-none"
      style={{ borderColor: color }}
      whileHover={{ y: -1 }}
      whileTap={{ scale: 0.995 }}
    >
      <span className="text-[15px]" style={{ color }}>
        {label}
      </span>
      <span
        className="text-[10px] tracking-widest px-1.5 py-[2px] border"
        style={{ borderColor: color, color }}
      >
        {kbd}
      </span>
    </motion.a>
  );
}

function Legend({ swatch, label }: { swatch: string; label: string }) {
  return (
    <div className="flex items-center gap-2">
      <span
        className="size-3 border"
        style={{ background: swatch, borderColor: swatch }}
      />
      <span
        className="text-[11px] uppercase tracking-wider"
        style={{ color: swatch }}
      >
        {label}
      </span>
    </div>
  );
}
