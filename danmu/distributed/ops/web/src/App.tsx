import React from "react";
import { usePoll, api, Health } from "./api";
import Overview from "./pages/Overview";
import Topology from "./pages/Topology";
import Services from "./pages/Services";
import Traffic from "./pages/Traffic";
import Rooms from "./pages/Rooms";
import KafkaPage from "./pages/Kafka";
import Messages from "./pages/Messages";
import Experiments from "./pages/Experiments";
import Sweeps from "./pages/Sweeps";
import RegimeAnalysis from "./pages/RegimeAnalysis";
import Compare from "./pages/Compare";
import Evidence from "./pages/Evidence";
import EventsPage from "./pages/Events";

// 轻量 hash 路由：页面少，不引入 react-router。
// 信息结构按产品建议：观测之上的实验层（Experiments / Sweeps / Analysis / Compare / Evidence）放在最前。
const PAGES: { path: string; label: string; indent?: boolean }[] = [
  { path: "overview", label: "Overview" },
  { path: "experiments", label: "Experiments" },
  { path: "sweeps", label: "Sweeps" },
  { path: "analysis/regimes", label: "Regime Analysis" },
  { path: "compare", label: "Compare" },
  { path: "evidence", label: "Evidence" },
  { path: "topology", label: "Topology" },
  { path: "traffic", label: "Traffic" },
  { path: "rooms", label: "Rooms" },
  { path: "kafka", label: "Kafka" },
  { path: "messages", label: "Messages" },
  { path: "events", label: "Events" },
  { path: "services", label: "Services" },
  { path: "services/comet", label: "Comet", indent: true },
  { path: "services/logic", label: "Logic", indent: true },
  { path: "services/job", label: "Job", indent: true },
  { path: "services/etcd", label: "Etcd", indent: true },
];

function useHashRoute(): string {
  const [hash, setHash] = React.useState(() => window.location.hash.replace(/^#\/?/, ""));
  React.useEffect(() => {
    const onChange = () => setHash(window.location.hash.replace(/^#\/?/, ""));
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);
  return hash || "overview";
}

function PageTitle({ route }: { route: string }) {
  const label = PAGES.find((p) => p.path === route)?.label ?? route;
  return (
    <div className="title">
      Danmu Distributed / <b>{label}</b>
    </div>
  );
}

export default function App() {
  const route = useHashRoute();
  // 顶栏健康状态用 overview 接口（任何页面都能看到全局健康）。
  const { data } = usePoll(api.overview, 2000);
  const health: Health = data?.health ?? "critical";
  const healthClass = data ? health : "unknown";

  let page: React.ReactNode;
  const effectiveRoute = route === "loadtest" ? "experiments" : route; // 旧 Load Test 入口 → Experiments
  if (effectiveRoute === "overview") page = <Overview />;
  else if (effectiveRoute === "experiments") page = <Experiments />;
  else if (effectiveRoute === "sweeps") page = <Sweeps />;
  else if (effectiveRoute === "analysis/regimes") page = <RegimeAnalysis />;
  else if (effectiveRoute === "compare") page = <Compare />;
  else if (effectiveRoute === "evidence") page = <Evidence />;
  else if (effectiveRoute === "topology") page = <Topology />;
  else if (effectiveRoute === "traffic") page = <Traffic />;
  else if (effectiveRoute === "rooms") page = <Rooms />;
  else if (effectiveRoute === "kafka") page = <KafkaPage />;
  else if (effectiveRoute === "messages") page = <Messages />;
  else if (effectiveRoute === "events") page = <EventsPage />;
  else if (effectiveRoute === "services") page = <Services />;
  else if (effectiveRoute.startsWith("services/")) page = <Services component={effectiveRoute.split("/")[1]} />;
  else page = <div className="hint">404: {route}</div>;

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="brand">
          Danmu Ops Console
          <span className="sub">distributed control plane</span>
        </div>
        <nav>
          {PAGES.map((p) =>
            p.indent ? null : (
              <a key={p.path} href={`#/${p.path}`} className={effectiveRoute === p.path ? "active" : ""}>
                {p.label}
              </a>
            )
          )}
          <div className="nav-group">Services</div>
          {PAGES.filter((p) => p.indent).map((p) => (
            <a key={p.path} href={`#/${p.path}`} className={`indent ${effectiveRoute === p.path ? "active" : ""}`}>
              {p.label}
            </a>
          ))}
        </nav>
      </aside>
      <div className="main">
        {data?.mock && <div className="mock-banner">MOCK DATA — 演示模式，数值不代表真实系统</div>}
        <header className="topbar">
          <PageTitle route={effectiveRoute} />
          <span className={`health-badge ${healthClass}`}>
            System {data ? health : "connecting…"}
          </span>
        </header>
        <main className="workspace">{page}</main>
      </div>
    </div>
  );
}
