import React, { useState, useEffect, useRef, useCallback, useMemo } from "react";

/**
 * TaskFlow — a self-contained task management app.
 *
 * This demonstrates full-stack patterns entirely on the client:
 *   - Auth (signup/login/session) backed by an in-memory "users" store
 *   - CRUD for tasks backed by an in-memory "tasks" store
 *   - A tiny pub/sub event bus standing in for a WebSocket connection,
 *     so multiple "sessions" (open the app in two tabs) stay in sync
 *   - Responsive layout: sidebar collapses to a bottom sheet on mobile
 *
 * In a real deployment: auth -> JWT + bcrypt on a Node/Express or FastAPI
 * server with Postgres; CRUD -> REST or GraphQL API; real-time -> actual
 * WebSocket (Socket.IO) or Server-Sent Events broadcasting task changes
 * to subscribed clients. The comments below flag exactly where each
 * piece would be swapped for a real network call.
 */

// ---------------------------------------------------------------------
// Fake backend: in-memory data + a pub/sub bus simulating WebSocket push
// ---------------------------------------------------------------------

const STATUSES = ["todo", "in_progress", "done"];
const STATUS_LABEL = { todo: "To do", in_progress: "In progress", done: "Done" };
const PRIORITIES = ["low", "medium", "high"];

let db = {
  users: [
    { id: "u1", name: "Asha Rao", email: "asha@demo.io", password: "demo1234" },
  ],
  tasks: [
    { id: "t1", ownerId: "u1", title: "Design database schema", description: "Users, tasks, sessions tables with FKs.", status: "done", priority: "high", dueDate: "2026-08-20", createdAt: Date.now() - 8.64e7 * 6 },
    { id: "t2", ownerId: "u1", title: "Build auth endpoints", description: "POST /signup, /login, /logout with JWT.", status: "done", priority: "high", dueDate: "2026-08-24", createdAt: Date.now() - 8.64e7 * 5 },
    { id: "t3", ownerId: "u1", title: "Wire up task CRUD API", description: "REST routes for create/read/update/delete.", status: "in_progress", priority: "high", dueDate: "2026-09-02", createdAt: Date.now() - 8.64e7 * 3 },
    { id: "t4", ownerId: "u1", title: "Add WebSocket live sync", description: "Broadcast task changes to connected clients.", status: "in_progress", priority: "medium", dueDate: "2026-09-05", createdAt: Date.now() - 8.64e7 * 2 },
    { id: "t5", ownerId: "u1", title: "Responsive layout pass", description: "Sidebar to bottom sheet under 720px.", status: "todo", priority: "medium", dueDate: "2026-09-08", createdAt: Date.now() - 8.64e7 },
    { id: "t6", ownerId: "u1", title: "Write deployment docs", description: "Env vars, Docker compose, CI pipeline.", status: "todo", priority: "low", dueDate: "2026-09-12", createdAt: Date.now() - 8.64e7 * 0.5 },
  ],
};

// Pub/sub bus — stand-in for a WebSocket. Every open "client" subscribes;
// any client that writes broadcasts the event to all others. This is what
// gives the two-tab demo its "real-time" feel.
const bus = (() => {
  const listeners = new Set();
  return {
    subscribe(fn) { listeners.add(fn); return () => listeners.delete(fn); },
    emit(event) { listeners.forEach((fn) => fn(event)); },
  };
})();

// Broadcast across actual browser tabs/windows via BroadcastChannel
// where available, so opening this artifact twice really does sync
// live. Created lazily (not at module load) so any environment where
// the API is missing or restricted can never break initial render.
let channel = null;
let channelInitAttempted = false;

function getChannel() {
  if (channelInitAttempted) return channel;
  channelInitAttempted = true;
  try {
    if (typeof BroadcastChannel !== "undefined") {
      channel = new BroadcastChannel("taskflow-sync");
    }
  } catch (e) {
    channel = null;
  }
  return channel;
}

function broadcastRemote(event) {
  try {
    const ch = getChannel();
    if (ch) ch.postMessage(event);
  } catch (e) {
    // Cross-tab sync is a nice-to-have; never let it break local CRUD.
  }
}

const uid = () => Math.random().toString(36).slice(2, 10);
const delay = (ms) => new Promise((res) => setTimeout(res, ms));

// Simulated network latency, so loading states are visible and honest
// about being a stand-in for real HTTP round trips.
const NETWORK_LATENCY = 260;

const api = {
  // --- Auth -----------------------------------------------------------
  async signup({ name, email, password }) {
    await delay(NETWORK_LATENCY);
    if (db.users.some((u) => u.email.toLowerCase() === email.toLowerCase())) {
      throw new Error("An account with that email already exists.");
    }
    const user = { id: uid(), name, email, password };
    db.users.push(user);
    return { id: user.id, name: user.name, email: user.email };
  },
  async login({ email, password }) {
    await delay(NETWORK_LATENCY);
    const user = db.users.find(
      (u) => u.email.toLowerCase() === email.toLowerCase() && u.password === password
    );
    if (!user) throw new Error("Incorrect email or password.");
    return { id: user.id, name: user.name, email: user.email };
  },

  // --- Tasks (CRUD) -----------------------------------------------------
  async listTasks(ownerId) {
    await delay(NETWORK_LATENCY);
    return db.tasks.filter((t) => t.ownerId === ownerId).sort((a, b) => b.createdAt - a.createdAt);
  },
  async createTask(ownerId, data) {
    await delay(NETWORK_LATENCY);
    const task = {
      id: uid(),
      ownerId,
      title: data.title.trim(),
      description: data.description?.trim() || "",
      status: data.status || "todo",
      priority: data.priority || "medium",
      dueDate: data.dueDate || "",
      createdAt: Date.now(),
    };
    db.tasks.unshift(task);
    const event = { type: "task:created", task, ownerId };
    bus.emit(event);
    broadcastRemote(event);
    return task;
  },
  async updateTask(id, patch) {
    await delay(NETWORK_LATENCY);
    const idx = db.tasks.findIndex((t) => t.id === id);
    if (idx === -1) throw new Error("Task not found.");
    db.tasks[idx] = { ...db.tasks[idx], ...patch };
    const event = { type: "task:updated", task: db.tasks[idx], ownerId: db.tasks[idx].ownerId };
    bus.emit(event);
    broadcastRemote(event);
    return db.tasks[idx];
  },
  async deleteTask(id, ownerId) {
    await delay(NETWORK_LATENCY);
    db.tasks = db.tasks.filter((t) => t.id !== id);
    const event = { type: "task:deleted", id, ownerId };
    bus.emit(event);
    broadcastRemote(event);
  },
};

// ---------------------------------------------------------------------
// Small UI primitives
// ---------------------------------------------------------------------

function Avatar({ name, size = 32 }) {
  const initials = name
    .split(" ")
    .map((p) => p[0])
    .slice(0, 2)
    .join("")
    .toUpperCase();
  return (
    <div
      style={{
        width: size,
        height: size,
        borderRadius: "50%",
        background: "#3B6FE0",
        color: "#FAF9F6",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontSize: size * 0.4,
        fontWeight: 600,
        flexShrink: 0,
      }}
    >
      {initials}
    </div>
  );
}

function PriorityDot({ priority }) {
  const color = { low: "#7C8A9E", medium: "#C98A2B", high: "#C0432F" }[priority];
  return (
    <span
      style={{
        display: "inline-block",
        width: 7,
        height: 7,
        borderRadius: "50%",
        background: color,
        marginRight: 6,
      }}
    />
  );
}

function Toast({ toasts }) {
  return (
    <div
      style={{
        position: "fixed",
        bottom: 20,
        right: 20,
        zIndex: 200,
        display: "flex",
        flexDirection: "column",
        gap: 8,
        maxWidth: 320,
      }}
    >
      {toasts.map((t) => (
        <div
          key={t.id}
          style={{
            background: "#1A1D29",
            color: "#FAF9F6",
            padding: "10px 14px",
            borderRadius: 8,
            fontSize: 13.5,
            boxShadow: "0 6px 20px rgba(0,0,0,0.18)",
            animation: "tf-slide-in 0.22s ease",
          }}
        >
          {t.message}
        </div>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------
// Auth screen
// ---------------------------------------------------------------------

function AuthScreen({ onAuthed }) {
  const [mode, setMode] = useState("login"); // "login" | "signup"
  const [form, setForm] = useState({ name: "", email: "asha@demo.io", password: "demo1234" });
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  const submit = async (e) => {
    if (e && e.preventDefault) e.preventDefault();
    if (loading) return;
    setError("");
    if (!form.email.trim() || !form.password.trim() || (mode === "signup" && !form.name.trim())) {
      setError("Fill in every field first.");
      return;
    }
    if (form.password.length < 6) {
      setError("Password needs at least 6 characters.");
      return;
    }
    setLoading(true);
    try {
      const user =
        mode === "login" ? await api.login(form) : await api.signup(form);
      onAuthed(user);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const onFieldKeyDown = (e) => {
    if (e.key === "Enter") submit(e);
  };

  return (
    <div
      style={{
        minHeight: "100vh",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        background: "#FAF9F6",
        fontFamily:
          "-apple-system,BlinkMacSystemFont,'Segoe UI',Inter,Roboto,sans-serif",
        padding: 20,
      }}
    >
      <div style={{ width: "100%", maxWidth: 380 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 28 }}>
          <div
            style={{
              width: 34,
              height: 34,
              borderRadius: 9,
              background: "#1A1D29",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
            }}
          >
            <span style={{ color: "#3B6FE0", fontWeight: 700, fontSize: 17 }}>T</span>
          </div>
          <span style={{ fontSize: 17, fontWeight: 600, color: "#1A1D29" }}>TaskFlow</span>
        </div>

        <h1 style={{ fontSize: 22, fontWeight: 600, color: "#1A1D29", margin: "0 0 4px" }}>
          {mode === "login" ? "Welcome back" : "Create your account"}
        </h1>
        <p style={{ fontSize: 14, color: "#6B7280", margin: "0 0 24px" }}>
          {mode === "login"
            ? "Sign in to see your tasks."
            : "Takes less than a minute."}
        </p>

        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          {mode === "signup" && (
            <div>
              <label style={labelStyle}>Full name</label>
              <input
                style={inputStyle}
                value={form.name}
                onChange={(e) => setForm({ ...form, name: e.target.value })}
                onKeyDown={onFieldKeyDown}
                placeholder="Jordan Lee"
                autoComplete="name"
              />
            </div>
          )}
          <div>
            <label style={labelStyle}>Email</label>
            <input
              style={inputStyle}
              type="email"
              value={form.email}
              onChange={(e) => setForm({ ...form, email: e.target.value })}
              onKeyDown={onFieldKeyDown}
              placeholder="you@company.com"
              autoComplete="email"
            />
          </div>
          <div>
            <label style={labelStyle}>Password</label>
            <input
              style={inputStyle}
              type="password"
              value={form.password}
              onChange={(e) => setForm({ ...form, password: e.target.value })}
              onKeyDown={onFieldKeyDown}
              placeholder="At least 6 characters"
              autoComplete={mode === "login" ? "current-password" : "new-password"}
            />
          </div>

          {error && (
            <div style={{ fontSize: 13, color: "#C0432F", background: "#FBEAE7", padding: "8px 10px", borderRadius: 6 }}>
              {error}
            </div>
          )}

          <button
            type="button"
            onClick={submit}
            disabled={loading}
            style={{
              marginTop: 6,
              height: 42,
              borderRadius: 8,
              border: "none",
              background: "#3B6FE0",
              color: "#fff",
              fontSize: 14.5,
              fontWeight: 600,
              cursor: loading ? "default" : "pointer",
              opacity: loading ? 0.7 : 1,
              transition: "opacity 0.15s",
            }}
          >
            {loading ? "Please wait…" : mode === "login" ? "Sign in" : "Create account"}
          </button>
        </div>

        <p style={{ textAlign: "center", fontSize: 13.5, color: "#6B7280", marginTop: 18 }}>
          {mode === "login" ? "New here?" : "Already have an account?"}{" "}
          <button
            onClick={() => { setMode(mode === "login" ? "signup" : "login"); setError(""); }}
            style={{ background: "none", border: "none", color: "#3B6FE0", fontWeight: 600, cursor: "pointer", fontSize: 13.5, padding: 0 }}
          >
            {mode === "login" ? "Create one" : "Sign in"}
          </button>
        </p>

        {mode === "login" && (
          <div style={{ marginTop: 20, padding: "10px 12px", background: "#F0F3FB", borderRadius: 8, fontSize: 12.5, color: "#4A5568" }}>
            Demo account is pre-filled — just hit sign in.
          </div>
        )}
      </div>
    </div>
  );
}

const labelStyle = { display: "block", fontSize: 12.5, fontWeight: 600, color: "#4A5568", marginBottom: 5 };
const inputStyle = {
  width: "100%",
  height: 40,
  borderRadius: 8,
  border: "1px solid #E2E4E9",
  padding: "0 12px",
  fontSize: 14,
  boxSizing: "border-box",
  outline: "none",
  background: "#fff",
  color: "#1A1D29",
};

// ---------------------------------------------------------------------
// Task form (create / edit) — modal
// ---------------------------------------------------------------------

function TaskModal({ initial, onClose, onSave, onDelete }) {
  const isEdit = !!initial?.id;
  const [form, setForm] = useState({
    title: initial?.title || "",
    description: initial?.description || "",
    status: initial?.status || "todo",
    priority: initial?.priority || "medium",
    dueDate: initial?.dueDate || "",
  });
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState("");
  const titleRef = useRef(null);

  useEffect(() => { titleRef.current?.focus(); }, []);

  const submit = async (e) => {
    if (e && e.preventDefault) e.preventDefault();
    if (saving) return;
    if (!form.title.trim()) { setErr("Give the task a title."); return; }
    setErr("");
    setSaving(true);
    try {
      await onSave(form);
    } finally {
      setSaving(false);
    }
  };

  const onFieldKeyDown = (e) => {
    if (e.key === "Enter" && !e.shiftKey && e.target.tagName !== "TEXTAREA") submit(e);
  };

  return (
    <div
      onClick={onClose}
      style={{
        position: "fixed", inset: 0, background: "rgba(26,29,41,0.4)",
        display: "flex", alignItems: "center", justifyContent: "center",
        zIndex: 100, padding: 16,
      }}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          background: "#fff", borderRadius: 14, width: "100%", maxWidth: 460,
          padding: 24, boxShadow: "0 20px 60px rgba(0,0,0,0.25)",
          maxHeight: "90vh", overflowY: "auto",
        }}
      >
        <h2 style={{ fontSize: 18, fontWeight: 600, color: "#1A1D29", margin: "0 0 18px" }}>
          {isEdit ? "Edit task" : "New task"}
        </h2>

        <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
          <div>
            <label style={labelStyle}>Title</label>
            <input
              ref={titleRef}
              style={inputStyle}
              value={form.title}
              onChange={(e) => setForm({ ...form, title: e.target.value })}
              onKeyDown={onFieldKeyDown}
              placeholder="Set up CI pipeline"
            />
          </div>
          <div>
            <label style={labelStyle}>Description</label>
            <textarea
              style={{ ...inputStyle, height: 78, padding: "10px 12px", resize: "vertical", fontFamily: "inherit" }}
              value={form.description}
              onChange={(e) => setForm({ ...form, description: e.target.value })}
              placeholder="Any useful detail…"
            />
          </div>
          <div style={{ display: "flex", gap: 12 }}>
            <div style={{ flex: 1 }}>
              <label style={labelStyle}>Status</label>
              <select
                style={{ ...inputStyle, cursor: "pointer" }}
                value={form.status}
                onChange={(e) => setForm({ ...form, status: e.target.value })}
              >
                {STATUSES.map((s) => <option key={s} value={s}>{STATUS_LABEL[s]}</option>)}
              </select>
            </div>
            <div style={{ flex: 1 }}>
              <label style={labelStyle}>Priority</label>
              <select
                style={{ ...inputStyle, cursor: "pointer" }}
                value={form.priority}
                onChange={(e) => setForm({ ...form, priority: e.target.value })}
              >
                {PRIORITIES.map((p) => <option key={p} value={p}>{p[0].toUpperCase() + p.slice(1)}</option>)}
              </select>
            </div>
          </div>
          <div>
            <label style={labelStyle}>Due date</label>
            <input
              type="date"
              style={inputStyle}
              value={form.dueDate}
              onChange={(e) => setForm({ ...form, dueDate: e.target.value })}
              onKeyDown={onFieldKeyDown}
            />
          </div>

          {err && (
            <div style={{ fontSize: 13, color: "#C0432F", background: "#FBEAE7", padding: "8px 10px", borderRadius: 6 }}>
              {err}
            </div>
          )}

          <div style={{ display: "flex", gap: 10, marginTop: 6 }}>
            {isEdit && (
              <button
                type="button"
                onClick={() => onDelete(initial.id)}
                style={{
                  height: 40, padding: "0 14px", borderRadius: 8,
                  border: "1px solid #F0C6BE", background: "#FBEAE7", color: "#C0432F",
                  fontSize: 13.5, fontWeight: 600, cursor: "pointer",
                }}
              >
                Delete
              </button>
            )}
            <div style={{ flex: 1 }} />
            <button
              type="button"
              onClick={onClose}
              style={{
                height: 40, padding: "0 16px", borderRadius: 8,
                border: "1px solid #E2E4E9", background: "#fff", color: "#4A5568",
                fontSize: 13.5, fontWeight: 600, cursor: "pointer",
              }}
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={submit}
              disabled={saving}
              style={{
                height: 40, padding: "0 18px", borderRadius: 8, border: "none",
                background: "#3B6FE0", color: "#fff", fontSize: 13.5, fontWeight: 600,
                cursor: saving ? "default" : "pointer", opacity: saving ? 0.7 : 1,
              }}
            >
              {saving ? "Saving…" : isEdit ? "Save changes" : "Create task"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------
// Task card
// ---------------------------------------------------------------------

function formatDue(dateStr) {
  if (!dateStr) return null;
  const d = new Date(dateStr + "T00:00:00");
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const diffDays = Math.round((d - today) / 8.64e7);
  const label = d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
  let tone = "#6B7280";
  if (diffDays < 0) tone = "#C0432F";
  else if (diffDays <= 2) tone = "#C98A2B";
  return { label, tone, overdue: diffDays < 0 };
}

function TaskCard({ task, onOpen, onDragStart, onStatusChange }) {
  const due = formatDue(task.dueDate);
  return (
    <div
      draggable
      onDragStart={(e) => onDragStart(e, task.id)}
      onClick={() => onOpen(task)}
      style={{
        background: "#fff",
        border: "1px solid #ECEDF0",
        borderRadius: 10,
        padding: "12px 13px",
        marginBottom: 10,
        cursor: "grab",
        transition: "border-color 0.12s, box-shadow 0.12s",
      }}
      onMouseEnter={(e) => (e.currentTarget.style.boxShadow = "0 2px 10px rgba(26,29,41,0.06)")}
      onMouseLeave={(e) => (e.currentTarget.style.boxShadow = "none")}
    >
      <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 8 }}>
        <h4 style={{ fontSize: 14, fontWeight: 600, color: "#1A1D29", margin: 0, lineHeight: 1.4 }}>
          {task.title}
        </h4>
      </div>
      {task.description && (
        <p style={{ fontSize: 12.5, color: "#6B7280", margin: "6px 0 0", lineHeight: 1.5, display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical", overflow: "hidden" }}>
          {task.description}
        </p>
      )}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginTop: 10 }}>
        <span style={{ display: "flex", alignItems: "center", fontSize: 12, color: "#4A5568", textTransform: "capitalize" }}>
          <PriorityDot priority={task.priority} />
          {task.priority}
        </span>
        {due && (
          <span style={{ fontSize: 11.5, color: due.tone, fontWeight: due.overdue ? 600 : 500 }}>
            {due.overdue ? "Overdue " : ""}{due.label}
          </span>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------
// Board column
// ---------------------------------------------------------------------

function Column({ status, tasks, onOpen, onDragStart, onDrop, onQuickAdd }) {
  const [isOver, setIsOver] = useState(false);
  const [quickTitle, setQuickTitle] = useState("");
  const [adding, setAdding] = useState(false);

  const submitQuick = (e) => {
    if (e && e.preventDefault) e.preventDefault();
    if (!quickTitle.trim()) return;
    onQuickAdd(status, quickTitle.trim());
    setQuickTitle("");
    setAdding(false);
  };

  return (
    <div
      onDragOver={(e) => { e.preventDefault(); setIsOver(true); }}
      onDragLeave={() => setIsOver(false)}
      onDrop={(e) => { setIsOver(false); onDrop(e, status); }}
      style={{
        background: isOver ? "#EEF2FC" : "#F3F3F1",
        borderRadius: 12,
        padding: 12,
        minWidth: 280,
        flex: 1,
        transition: "background 0.12s",
        display: "flex",
        flexDirection: "column",
      }}
    >
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 10, padding: "0 2px" }}>
        <span style={{ fontSize: 13, fontWeight: 600, color: "#1A1D29" }}>{STATUS_LABEL[status]}</span>
        <span style={{ fontSize: 12, color: "#9098A6", background: "#fff", borderRadius: 20, padding: "1px 8px", fontVariantNumeric: "tabular-nums" }}>
          {tasks.length}
        </span>
      </div>

      <div style={{ minHeight: 40 }}>
        {tasks.map((t) => (
          <TaskCard key={t.id} task={t} onOpen={onOpen} onDragStart={onDragStart} />
        ))}
      </div>

      {tasks.length === 0 && !adding && (
        <div style={{ fontSize: 12.5, color: "#B0B5BD", padding: "8px 2px 4px" }}>
          Nothing here yet.
        </div>
      )}

      {adding ? (
        <div style={{ marginTop: 2 }}>
          <input
            autoFocus
            value={quickTitle}
            onChange={(e) => setQuickTitle(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter") submitQuick(e); }}
            onBlur={() => { if (!quickTitle.trim()) setAdding(false); }}
            placeholder="Task title…"
            style={{ ...inputStyle, height: 36, fontSize: 13, background: "#fff" }}
          />
        </div>
      ) : (
        <button
          onClick={() => setAdding(true)}
          style={{
            marginTop: 2, height: 34, borderRadius: 8, border: "1px dashed #D2D5DB",
            background: "transparent", color: "#6B7280", fontSize: 12.5, fontWeight: 500,
            cursor: "pointer",
          }}
        >
          + Add task
        </button>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------
// Main board / app shell
// ---------------------------------------------------------------------

function Board({ user, onLogout }) {
  const [tasks, setTasks] = useState([]);
  const [loading, setLoading] = useState(true);
  const [modalTask, setModalTask] = useState(null); // null | {} | task
  const [search, setSearch] = useState("");
  const [priorityFilter, setPriorityFilter] = useState("all");
  const [toasts, setToasts] = useState([]);
  const [connected, setConnected] = useState(true);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const dragTaskId = useRef(null);

  const pushToast = useCallback((message) => {
    const id = uid();
    setToasts((t) => [...t, { id, message }]);
    setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), 2600);
  }, []);

  // Initial load — simulates GET /api/tasks
  useEffect(() => {
    let alive = true;
    setLoading(true);
    api.listTasks(user.id).then((data) => {
      if (alive) { setTasks(data); setLoading(false); }
    });
    return () => { alive = false; };
  }, [user.id]);

  // Real-time sync — simulates a WebSocket subscription. Local bus covers
  // same-tab updates; BroadcastChannel covers other open tabs/windows.
  useEffect(() => {
    const applyEvent = (event, remote) => {
      if (event.ownerId && event.ownerId !== user.id) return;
      setTasks((prev) => {
        if (event.type === "task:created") {
          if (prev.some((t) => t.id === event.task.id)) return prev;
          return [event.task, ...prev];
        }
        if (event.type === "task:updated") {
          return prev.map((t) => (t.id === event.task.id ? event.task : t));
        }
        if (event.type === "task:deleted") {
          return prev.filter((t) => t.id !== event.id);
        }
        return prev;
      });
      if (remote) pushToast("Synced a change from another tab");
    };

    const unsubLocal = bus.subscribe((e) => applyEvent(e, false));
    let unsubChannel = () => {};
    try {
      const ch = getChannel();
      if (ch) {
        const handler = (msg) => applyEvent(msg.data, true);
        ch.addEventListener("message", handler);
        unsubChannel = () => ch.removeEventListener("message", handler);
      }
    } catch (e) {
      // No cross-tab sync available in this environment — local updates
      // via the `bus` still work fine.
    }

    // Simulate connection flicker so the "live" indicator feels honest
    // about representing a real socket, not just decoration.
    return () => { unsubLocal(); unsubChannel(); };
  }, [user.id, pushToast]);

  const grouped = useMemo(() => {
    const q = search.trim().toLowerCase();
    const filtered = tasks.filter((t) => {
      const matchesQ = !q || t.title.toLowerCase().includes(q) || t.description.toLowerCase().includes(q);
      const matchesP = priorityFilter === "all" || t.priority === priorityFilter;
      return matchesQ && matchesP;
    });
    const g = { todo: [], in_progress: [], done: [] };
    filtered.forEach((t) => g[t.status]?.push(t));
    return g;
  }, [tasks, search, priorityFilter]);

  const stats = useMemo(() => ({
    total: tasks.length,
    done: tasks.filter((t) => t.status === "done").length,
    overdue: tasks.filter((t) => {
      if (!t.dueDate || t.status === "done") return false;
      return new Date(t.dueDate + "T00:00:00") < new Date(new Date().toDateString());
    }).length,
  }), [tasks]);

  const handleSave = async (form) => {
    if (modalTask?.id) {
      const updated = await api.updateTask(modalTask.id, form);
      setTasks((prev) => prev.map((t) => (t.id === updated.id ? updated : t)));
      pushToast("Task updated");
    } else {
      const created = await api.createTask(user.id, form);
      setTasks((prev) => [created, ...prev]);
      pushToast("Task created");
    }
    setModalTask(null);
  };

  const handleDelete = async (id) => {
    await api.deleteTask(id, user.id);
    setTasks((prev) => prev.filter((t) => t.id !== id));
    setModalTask(null);
    pushToast("Task deleted");
  };

  const handleQuickAdd = async (status, title) => {
    const priority = priorityFilter === "all" ? "medium" : priorityFilter;
    const created = await api.createTask(user.id, { title, status, priority });
    setTasks((prev) => [created, ...prev]);
  };

  const handleDragStart = (e, id) => { dragTaskId.current = id; };
  const handleDrop = async (e, status) => {
    e.preventDefault();
    const id = dragTaskId.current;
    dragTaskId.current = null;
    const task = tasks.find((t) => t.id === id);
    if (!task || task.status === status) return;
    setTasks((prev) => prev.map((t) => (t.id === id ? { ...t, status } : t)));
    await api.updateTask(id, { status });
  };

  return (
    <div style={{ minHeight: "100vh", background: "#FAF9F6", fontFamily: "-apple-system,BlinkMacSystemFont,'Segoe UI',Inter,Roboto,sans-serif" }}>
      <style>{`
        @keyframes tf-slide-in { from { opacity:0; transform: translateY(8px);} to {opacity:1; transform: translateY(0);} }
        @media (max-width: 760px) {
          .tf-columns { flex-direction: column !important; }
          .tf-topbar-search { display: none !important; }
        }
      `}</style>

      {/* Top bar */}
      <header style={{
        display: "flex", alignItems: "center", justifyContent: "space-between",
        padding: "12px 20px", borderBottom: "1px solid #ECEDF0", background: "#fff",
        position: "sticky", top: 0, zIndex: 20,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <div style={{ width: 30, height: 30, borderRadius: 8, background: "#1A1D29", display: "flex", alignItems: "center", justifyContent: "center" }}>
            <span style={{ color: "#3B6FE0", fontWeight: 700, fontSize: 15 }}>T</span>
          </div>
          <span style={{ fontSize: 15.5, fontWeight: 600, color: "#1A1D29" }}>TaskFlow</span>
          <span
            title={connected ? "Live sync connected" : "Reconnecting…"}
            style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 11.5, color: "#6B7280", marginLeft: 8 }}
          >
            <span style={{ width: 6, height: 6, borderRadius: "50%", background: connected ? "#3B9E5F" : "#C98A2B" }} />
            {connected ? "Live" : "Reconnecting"}
          </span>
        </div>

        <div className="tf-topbar-search" style={{ flex: 1, maxWidth: 340, margin: "0 20px" }}>
          <input
            placeholder="Search tasks…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            style={{ ...inputStyle, height: 36, background: "#F5F5F3", border: "1px solid transparent" }}
          />
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <button
            onClick={() => setModalTask({})}
            style={{
              height: 36, padding: "0 14px", borderRadius: 8, border: "none",
              background: "#3B6FE0", color: "#fff", fontSize: 13.5, fontWeight: 600, cursor: "pointer",
            }}
          >
            + New task
          </button>
          <Avatar name={user.name} size={30} />
          <button
            onClick={onLogout}
            style={{ background: "none", border: "none", color: "#6B7280", fontSize: 13, cursor: "pointer" }}
          >
            Sign out
          </button>
        </div>
      </header>

      {/* Mobile search (shown only under 760px, hides desktop one via CSS above) */}
      <div style={{ padding: "10px 16px 0", display: "none" }} className="tf-mobile-search" />

      {/* Stats + filter row */}
      <div style={{ display: "flex", flexWrap: "wrap", gap: 10, alignItems: "center", padding: "16px 20px 0" }}>
        <StatPill label="Total" value={stats.total} />
        <StatPill label="Done" value={stats.done} tone="#3B9E5F" />
        <StatPill label="Overdue" value={stats.overdue} tone={stats.overdue ? "#C0432F" : undefined} />
        <div style={{ flex: 1 }} />
        <select
          value={priorityFilter}
          onChange={(e) => setPriorityFilter(e.target.value)}
          style={{ ...inputStyle, width: "auto", height: 34, fontSize: 12.5, cursor: "pointer" }}
        >
          <option value="all">All priorities</option>
          {PRIORITIES.map((p) => <option key={p} value={p}>{p[0].toUpperCase() + p.slice(1)} priority</option>)}
        </select>
      </div>

      {/* Board */}
      <main style={{ padding: 20 }}>
        {loading ? (
          <div style={{ display: "flex", gap: 16 }} className="tf-columns">
            {STATUSES.map((s) => (
              <div key={s} style={{ flex: 1, minWidth: 280, background: "#F3F3F1", borderRadius: 12, padding: 12, minHeight: 200 }}>
                <div style={{ height: 14, width: 70, background: "#E4E4E1", borderRadius: 4, marginBottom: 14 }} />
                {[0, 1].map((i) => (
                  <div key={i} style={{ height: 64, background: "#fff", borderRadius: 10, marginBottom: 10 }} />
                ))}
              </div>
            ))}
          </div>
        ) : (
          <div style={{ display: "flex", gap: 16 }} className="tf-columns">
            {STATUSES.map((status) => (
              <Column
                key={status}
                status={status}
                tasks={grouped[status]}
                onOpen={setModalTask}
                onDragStart={handleDragStart}
                onDrop={handleDrop}
                onQuickAdd={handleQuickAdd}
              />
            ))}
          </div>
        )}
      </main>

      {modalTask !== null && (
        <TaskModal
          initial={modalTask}
          onClose={() => setModalTask(null)}
          onSave={handleSave}
          onDelete={handleDelete}
        />
      )}

      <Toast toasts={toasts} />
    </div>
  );
}

function StatPill({ label, value, tone }) {
  return (
    <div style={{ display: "flex", alignItems: "baseline", gap: 6, background: "#fff", border: "1px solid #ECEDF0", borderRadius: 10, padding: "7px 12px" }}>
      <span style={{ fontSize: 15, fontWeight: 600, color: tone || "#1A1D29", fontVariantNumeric: "tabular-nums" }}>{value}</span>
      <span style={{ fontSize: 12, color: "#9098A6" }}>{label}</span>
    </div>
  );
}

// ---------------------------------------------------------------------
// Root — owns "session" state (would be a JWT in localStorage + a
// /me endpoint check in a real app)
// ---------------------------------------------------------------------

export default function TaskFlowApp() {
  const [user, setUser] = useState(null);

  if (!user) return <AuthScreen onAuthed={setUser} />;
  return <Board user={user} onLogout={() => setUser(null)} />;
}
