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
  const [openKey, setOpenKey] = useState<string | null>("I");
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
      <section className="grid items-center gap-5 md:gap-8 md:grid-cols-[58ch_1fr] mx-auto w-full max-w-[900px] lg:max-w-[1100px] xl:max-w-[1200px]">
        <div className="max-w-[58ch] order-2 md:order-1">
          <motion.p
            initial={{ opacity: 0, y: 4 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.05, duration: 0.5 }}
            className="text-[28px] leading-tight md:text-[32px] md:leading-tight font-normal"
          >
            We find warm funders that are likely to support your work.
          </motion.p>

          <div className="mt-6 md:mt-8 grid gap-3">
            <NavItem
              label="Get Started"
              kbd="I"
              color={palette.ink}
              isOpen={openKey === "I"}
              onToggle={() => toggle("I")}
              mountDelay={0.05}
            >
              <div className="grid gap-3">
                {/* Starter Package */}
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <div
                      className="text-[14px] leading-relaxed"
                      style={{ color: palette.ink }}
                    >
                      Starter Package — $289
                    </div>
                    <div
                      className="text-[12px]"
                      style={{ color: palette.ink, opacity: 0.8 }}
                    >
                      30–50 leads, 7-day turnaround
                    </div>
                  </div>
                  <a
                    href="https://buy.stripe.com/00wcN77ZHe3X9ov6vm4ow00" // TODO: replace with your Stripe Checkout link or /api/checkout
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-block border px-4 py-2 text-[13px] uppercase tracking-[0.14em] bg-[#0098d8] text-white border-[#0098d8] transition-colors transition-transform hover:bg-[#007fb6] hover:border-[#007fb6] hover:-translate-y-[1px] focus-visible:-translate-y-[1px] focus-visible:ring-2 focus-visible:ring-[#0098d8] focus-visible:ring-offset-2 outline-none"
                  >
                    Pay with Stripe
                  </a>
                </div>

                {/* Custom Scope */}
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <div
                      className="text-[14px] leading-relaxed"
                      style={{ color: palette.ink }}
                    >
                      Custom scope
                    </div>
                    <div
                      className="text-[12px]"
                      style={{ color: palette.ink, opacity: 0.8 }}
                    >
                      Book a quick call to scope your needs
                    </div>
                  </div>
                  <a
                    href="https://calendly.com/your-handle/15min" // TODO: replace with your Calendly URL
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-block border px-4 py-2 text-[13px] uppercase tracking-[0.14em] text-[#0b3536] border-[#0b3536] transition-transform hover:-translate-y-[1px] focus-visible:-translate-y-[1px] focus-visible:ring-2 focus-visible:ring-[#0098d8] focus-visible:ring-offset-2 outline-none"
                  >
                    Book 15-min call
                  </a>
                </div>
              </div>
            </NavItem>

            <NavItem
              label="Package"
              kbd="P"
              color={palette.blue}
              isOpen={openKey === "P"}
              onToggle={() => toggle("P")}
              mountDelay={0.1}
            >
              <p className="text-[14px] leading-relaxed">
                Get 30 to 50 warm private-foundation leads tailored to your org.
                $289. Delivered in 72 hours.
              </p>
              <a
                href="/example.pdf"
                target="_blank"
                rel="noopener noreferrer"
                className="inline-block mt-3 border px-4 py-2 text-[13px] uppercase tracking-[0.14em] bg-[#0098d8] text-white border-[#0098d8] transition-colors transition-transform hover:bg-[#007fb6] hover:border-[#007fb6] hover:-translate-y-[1px] focus-visible:-translate-y-[1px] focus-visible:ring-2 focus-visible:ring-[#0098d8] focus-visible:ring-offset-2 outline-none"
              >
                See an example
              </a>
            </NavItem>

            <NavItem
              label="Methods"
              kbd="M"
              color={palette.red}
              isOpen={openKey === "M"}
              onToggle={() => toggle("M")}
              mountDelay={0.15}
            >
              <p className="text-[14px] leading-relaxed">
                We use data science to search through IRS filings, recent press,
                public grants, and board membership. We leverage this data to
                identify potential funders-- and warm introduction paths for
                your nonprofit.
              </p>
            </NavItem>
          </div>
        </div>

        {/* Vignette */}
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
  mountDelay = 0,
}: {
  label: string;
  kbd: string;
  color: string;
  isOpen: boolean;
  onToggle: () => void;
  children?: ReactNode;
  mountDelay?: number;
}) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 6 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{
        delay: mountDelay,
        duration: 0.45,
        ease: [0.2, 0.8, 0.2, 1],
      }}
    >
      <motion.button
        type="button"
        onClick={onToggle}
        aria-expanded={isOpen}
        initial="rest"
        animate="rest"
        whileHover="hover"
        transition={{ duration: 0.4 }}
        className="group flex w-full items-center justify-between border px-3 py-2 select-none text-left cursor-pointer outline-none focus:outline-none focus-visible:outline-none focus:ring-0 focus-visible:ring-0 focus:ring-offset-0 focus-visible:ring-offset-0"
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
    </motion.div>
  );
}

function Vignette() {
  return (
    <motion.div
      initial={{ opacity: 0, y: 6 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: 0.2, duration: 0.5 }}
      className="relative block order-1 md:order-2 justify-self-center md:justify-self-end pointer-events-none"
      aria-hidden="true"
    >
      <div className="relative h-[360px] w-[260px] lg:h-[440px] lg:w-[320px]">
        {/* back sheet */}
        <div
          className="absolute left-6 top-6 h-[360px] w-[260px] lg:h-[440px] lg:w-[320px] -rotate-2 border rounded-[6px] shadow-md"
          style={{ background: palette.paper, borderColor: palette.ink }}
        >
          <div
            className="h-6 rounded-t-[6px]"
            style={{ background: palette.blue }}
          />
        </div>

        {/* front sheet */}
        <div
          className="absolute left-0 top-0 h-[360px] w-[260px] lg:h-[440px] lg:w-[320px] rotate-3 border rounded-[6px] shadow-md"
          style={{ background: palette.paper, borderColor: palette.ink }}
        >
          <div
            className="h-6 rounded-t-[6px]"
            style={{ background: palette.red }}
          />
          <div className="p-4 space-y-3">
            <div
              className="text-[10px] uppercase tracking-[0.22em] leading-none"
              style={{ color: palette.ink, opacity: 0.6 }}
            >
              Funder Shortlist
            </div>

            <Entry
              title="Acme Family Foundation"
              noteLabel="Why it fits"
              noteText="Gave to Peer Organization (’24)"
            />
            <Entry
              title="Northwind Trust"
              noteLabel="Warm path"
              noteText="Board overlap via J. Smith"
            />
            {/* Hide this on mobile to prevent overflow */}
            <div className="hidden lg:block">
              <Entry
                title="Globex Philanthropies"
                noteLabel="Average grant"
                noteText="$55k · Cycle: Oct–Nov"
              />
            </div>
          </div>
        </div>
      </div>
    </motion.div>
  );
}

// Minimal entry block with title + skeleton paragraphs + optional note
function Entry({
  title,
  noteLabel,
  noteText,
}: {
  title: string;
  noteLabel?: string;
  noteText?: string;
}) {
  return (
    <div className="space-y-2">
      <div
        className="text-[12px] uppercase tracking-[0.18em] leading-none"
        style={{ color: palette.ink }}
      >
        {title}
      </div>
      {noteLabel && noteText && (
        <div className="pt-0.5">
          <span
            className="text-[10px] uppercase tracking-[0.18em] mr-1"
            style={{ color: palette.ink, opacity: 0.65 }}
          >
            {noteLabel}:
          </span>
          <span
            className="text-[11px] leading-snug"
            style={{ color: palette.ink, opacity: 0.85 }}
          >
            {noteText}
          </span>
        </div>
      )}
      <div className="space-y-1.5">
        <div
          className="h-2 w-5/6 rounded"
          style={{ background: palette.ink, opacity: 0.15 }}
        />
        <div
          className="h-2 w-4/6 rounded"
          style={{ background: palette.ink, opacity: 0.15 }}
        />
        <div
          className="h-2 w-3/5 rounded"
          style={{ background: palette.ink, opacity: 0.15 }}
        />
        <div
          className="h-2 w-2/3 rounded"
          style={{ background: palette.ink, opacity: 0.15 }}
        />
      </div>
    </div>
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
