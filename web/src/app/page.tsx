"use client";

import { motion, AnimatePresence } from "framer-motion";
import { useEffect, useState, type ReactNode } from "react";

const palette = {
  blue: "#0098d8",
  red: "#f54123",
  paper: "#e5e7de",
  ink: "#0b3536",
} as const;

export default function Page() {
  const [openKey, setOpenKey] = useState<string | null>(null);
  const toggle = (k: string) => setOpenKey((prev) => (prev === k ? null : k));

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const k = e.key.toUpperCase();
      if (k === "I" || k === "P" || k === "M") {
        e.preventDefault();
        toggle(k);
      } else if (k === "ESCAPE") {
        setOpenKey(null);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

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
          FIND FUNDERS
        </motion.h1>
        <motion.span
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.2, duration: 0.6 }}
          className="text-[11px] uppercase tracking-[0.18em]"
          style={{ color: palette.blue }}
        >
          field manual / 2025
        </motion.span>
      </header>

      {/* Body */}
      <section className="grid items-center gap-8 md:grid-cols-[58ch_1fr] mx-auto w-full max-w-[900px] lg:max-w-[1100px] xl:max-w-[1200px]">
        <div className="max-w-[58ch]">
          <motion.p
            initial={{ opacity: 0, y: 4 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.05, duration: 0.5 }}
            className="text-[28px] leading-tight md:text-[32px] md:leading-tight font-normal"
          >
            We find warm funders that are likely to support your work.
          </motion.p>

          <div className="mt-8 grid gap-3">
            <NavItem
              label="Get Started"
              kbd="I"
              color={palette.ink}
              isOpen={openKey === "I"}
              onToggle={() => toggle("I")}
            >
              <form className="grid gap-3">
                <div className="grid gap-1">
                  <label className="text-[11px] uppercase tracking-[0.18em]">
                    Name
                  </label>
                  <input
                    type="text"
                    className="border px-2 py-1 text-[14px] bg-transparent outline-none"
                    style={{ borderColor: palette.ink, color: palette.ink }}
                    placeholder="Jane Smith"
                  />
                </div>
                <div className="grid gap-1">
                  <label className="text-[11px] uppercase tracking-[0.18em]">
                    Email
                  </label>
                  <input
                    type="email"
                    className="border px-2 py-1 text-[14px] bg-transparent outline-none"
                    style={{ borderColor: palette.ink, color: palette.ink }}
                    placeholder="jane@org.org"
                  />
                </div>
                <div className="grid gap-1">
                  <label className="text-[11px] uppercase tracking-[0.18em]">
                    Organization
                  </label>
                  <input
                    type="text"
                    className="border px-2 py-1 text-[14px] bg-transparent outline-none"
                    style={{ borderColor: palette.ink, color: palette.ink }}
                    placeholder="Optional"
                  />
                </div>
                <button
                  type="submit"
                  className="justify-self-start border px-3 py-1 text-[12px] uppercase tracking-[0.14em] transition-transform"
                  style={{ borderColor: palette.ink, color: palette.ink }}
                >
                  Submit
                </button>
              </form>
            </NavItem>

            <NavItem
              label="Package"
              kbd="P"
              color={palette.blue}
              isOpen={openKey === "P"}
              onToggle={() => toggle("P")}
            >
              <p className="text-[14px] leading-relaxed">
                We prepare a concise funder brief and outreach plan tailored to
                your goals. Deliverables and timelines are calibrated to your
                scope.
              </p>
            </NavItem>

            <NavItem
              label="Methods"
              kbd="M"
              color={palette.red}
              isOpen={openKey === "M"}
              onToggle={() => toggle("M")}
            >
              <p className="text-[14px] leading-relaxed">
                We map adjacent funders, trace warm paths, and prioritize
                prospects by fit, timing, and likelihood of support.
              </p>
            </NavItem>
          </div>
        </div>

        <Vignette />
      </section>

      {/* Footer */}
      <footer
        className="flex items-center justify-between pt-6 border-t"
        style={{ borderColor: "#0b3536" }}
      >
        <small className="text-[11px] tracking-wide">
          © {new Date().getFullYear()} Find Funders
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
  isOpen,
  onToggle,
  children,
}: {
  label: string;
  kbd: string;
  color: string;
  isOpen: boolean;
  onToggle: () => void;
  children?: ReactNode;
}) {
  return (
    <div>
      <motion.button
        type="button"
        onClick={onToggle}
        aria-expanded={isOpen}
        initial="rest"
        animate="rest"
        whileHover="hover"
        transition={{ duration: 0.4 }}
        className="group flex w-full items-center justify-between border px-3 py-2 select-none text-left cursor-pointer"
        style={{ borderColor: color }}
        whileTap={{ scale: 0.995 }}
      >
        <span className="text-[15px] relative inline-block" style={{ color }}>
          {label}
          <motion.span
            variants={{
              rest: { scaleX: 0, opacity: 0 },
              hover: { scaleX: 1, opacity: 1 },
            }}
            transition={{ duration: 0.25, ease: [0.2, 0.8, 0.2, 1] }}
            className="absolute left-0 -bottom-[2px] h-px"
            style={{
              width: "100%",
              backgroundColor: color,
              transformOrigin: "left",
            }}
          />
        </span>
        <span
          className="text-[10px] tracking-widest px-1.5 py-[2px] border"
          style={{ borderColor: color, color }}
        >
          {kbd}
        </span>
      </motion.button>

      <AnimatePresence initial={false}>
        {isOpen && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.35, ease: [0.2, 0.8, 0.2, 1] }}
            className="overflow-hidden"
          >
            <div
              className="mt-1 border px-3 py-3"
              style={{ borderColor: color, background: "transparent" }}
            >
              {children}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

function Vignette() {
  return (
    <motion.div
      initial={{ opacity: 0, y: 6 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: 0.2, duration: 0.5 }}
      className="relative hidden md:block justify-self-center md:justify-self-end pointer-events-none"
      aria-hidden="true"
    >
      <div className="relative h-[360px] w-[260px] lg:h-[440px] lg:w-[320px]">
        <div
          className="absolute left-6 top-6 h-[360px] w-[260px] lg:h-[440px] lg:w-[320px] -rotate-2 border rounded-[6px] shadow-md"
          style={{ background: palette.paper, borderColor: palette.ink }}
        >
          <div
            className="h-6 rounded-t-[6px]"
            style={{ background: palette.blue }}
          />
        </div>

        <div
          className="absolute left-0 top-0 h-[360px] w-[260px] lg:h-[440px] lg:w-[320px] rotate-3 border rounded-[6px] shadow-md"
          style={{ background: palette.paper, borderColor: palette.ink }}
        >
          <div
            className="h-6 rounded-t-[6px]"
            style={{ background: palette.red }}
          />
          <div className="p-4 space-y-2">
            <div
              className="h-3 w-2/3 rounded"
              style={{ background: palette.ink, opacity: 0.15 }}
            />
            <div
              className="h-3 w-1/2 rounded"
              style={{ background: palette.ink, opacity: 0.15 }}
            />
            <div
              className="h-3 w-5/6 rounded"
              style={{ background: palette.ink, opacity: 0.15 }}
            />
          </div>
        </div>
      </div>
    </motion.div>
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
