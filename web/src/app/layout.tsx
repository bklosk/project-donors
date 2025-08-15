import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Project Donors",
  description: "Field-manual minimal placeholder",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="h-full">
      <body className="h-full antialiased text-[15px] leading-snug">
        {children}
      </body>
    </html>
  );
}
