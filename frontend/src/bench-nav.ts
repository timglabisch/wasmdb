export function renderNav(active: "xstate" | "wasmdb" | "both") {
  const nav = document.createElement("div");
  nav.style.cssText = "display:flex;gap:8px;padding:16px 32px;border-bottom:1px solid #ccc;font-family:monospace;";
  const links = [
    { href: "/bench-xstate.html", label: "xstate only", key: "xstate" as const },
    { href: "/bench-wasmdb.html", label: "wasmdb only", key: "wasmdb" as const },
    { href: "/bench-projections.html", label: "both", key: "both" as const },
  ];
  for (const l of links) {
    const a = document.createElement("a");
    a.href = l.href;
    a.textContent = l.label;
    a.style.cssText = l.key === active
      ? "font-weight:bold;text-decoration:none;color:#000;"
      : "text-decoration:underline;color:#06c;";
    nav.appendChild(a);
  }
  document.body.prepend(nav);
}
