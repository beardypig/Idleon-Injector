const { createLogger } = require("../utils/logger");

const log = createLogger("Monitor");

/** Own Monitor lifecycle state; clients are opaque tokens supplied by the transport. */
function createMonitor({ gameBridge, sendState, clock = { setTimeout, clearTimeout, now: Date.now } }) {
    /** @type {Map<object, Map<string, { path: string, history: Array<{ value: any, ts: number }>, error?: string }>>} */
    const clientMonitorState = new Map();

    /**
     * Game-side wraps currently installed, keyed by path. Subscriber counts are
     * always derived from clientMonitorState so competing subscribe/cleanup
     * interleavings cannot drift out of sync with reality.
     * @type {Map<string, { id: string, releasePromise?: Promise<void>|null }>}
     */
    const globalWatchersByPath = new Map();

    const HISTORY_LIMIT = 10;
    const MONITOR_SUBSCRIBE_RETRY_DELAY_MS = 1000;
    const MONITOR_SUBSCRIBE_MAX_RETRIES = 20;
    // shortcut: coalesced full-state sends; switch to per-id delta messages if
    // clients ever watch enough paths for serialization to matter
    const MONITOR_SEND_COALESCE_MS = 100;

    /** @type {Map<object, NodeJS.Timeout>} */
    const monitorSendTimers = new Map();

    /** @type {Map<object, Map<string, NodeJS.Timeout>>} */
    const monitorSubscribeRetryTimers = new Map();

    function monitorIdFromPath(path) {
        return "mon:" + encodeURIComponent(path);
    }

    function pushHistory(entry, value, ts = clock.now()) {
        entry.history.unshift({ value, ts });
        if (entry.history.length > HISTORY_LIMIT) {
            entry.history.pop();
        }
    }

    function getClientMonitorMap(client) {
        let monitorMap = clientMonitorState.get(client);
        if (!monitorMap) {
            monitorMap = new Map();
            clientMonitorState.set(client, monitorMap);
        }
        return monitorMap;
    }

    function getClientRetryMap(client) {
        let retryMap = monitorSubscribeRetryTimers.get(client);
        if (!retryMap) {
            retryMap = new Map();
            monitorSubscribeRetryTimers.set(client, retryMap);
        }
        return retryMap;
    }

    function countMonitorSubscribers(id) {
        let count = 0;
        for (const monitorMap of clientMonitorState.values()) {
            if (monitorMap.has(id)) {
                count += 1;
            }
        }
        return count;
    }

    function countStaticSubscribers(id) {
        let count = 0;
        for (const monitorMap of clientMonitorState.values()) {
            for (const [monitorId, entry] of monitorMap.entries()) {
                if (monitorId === id && entry.staticEnabled) count += 1;
            }
        }
        return count;
    }

    function clearMonitorSubscribeRetry(client, id) {
        const retryMap = monitorSubscribeRetryTimers.get(client);
        if (!retryMap) return;

        const timer = retryMap.get(id);
        if (timer) {
            clock.clearTimeout(timer);
            retryMap.delete(id);
        }

        if (retryMap.size === 0) {
            monitorSubscribeRetryTimers.delete(client);
        }
    }

    function clearAllMonitorSubscribeRetries(client) {
        const retryMap = monitorSubscribeRetryTimers.get(client);
        if (!retryMap) return;

        for (const timer of retryMap.values()) {
            clock.clearTimeout(timer);
        }

        monitorSubscribeRetryTimers.delete(client);
    }

    function isTransientMonitorSubscribeError(errorText) {
        if (!errorText) return false;

        const text = String(errorText);
        return text.includes("Target object is null or undefined") || text.includes("Cannot resolve path segment");
    }

    function scheduleMonitorSubscribeRetry(client, path, id, retryAttempt) {
        if (retryAttempt >= MONITOR_SUBSCRIBE_MAX_RETRIES) {
            failMonitorSubscription(client, id, `Gave up after ${MONITOR_SUBSCRIBE_MAX_RETRIES} retries`);
            return;
        }

        const retryMap = getClientRetryMap(client);
        if (retryMap.has(id)) return;

        const timer = clock.setTimeout(() => {
            retryMap.delete(id);
            if (retryMap.size === 0) {
                monitorSubscribeRetryTimers.delete(client);
            }

            const monitorMap = clientMonitorState.get(client);
            const entry = monitorMap?.get(id);
            if (!entry) return;
            void handleMonitorSubscribe(client, path, entry.staticEnabled, retryAttempt + 1, true);
        }, MONITOR_SUBSCRIBE_RETRY_DELAY_MS);

        retryMap.set(id, timer);
    }

    /**
     * Parks a client subscription with an error instead of deleting it: a deleted
     * entry makes the UI's reconcile resubscribe immediately, which loops forever
     * for permanently failing paths. A parked entry is retried only when the user
     * toggles the watcher or the game context is replaced.
     */
    function failMonitorSubscription(client, id, errorText) {
        clearMonitorSubscribeRetry(client, id);

        const monitorMap = clientMonitorState.get(client);
        const entry = monitorMap && monitorMap.get(id);
        if (entry) {
            entry.error = String(errorText || "Monitor subscribe failed");
            sendMonitorStateToClient(client);
        }

        log.error(`Monitor subscribe failed for ${id}: ${errorText}`);
    }

    function sendMonitorStateToClient(client) {
        const pendingTimer = monitorSendTimers.get(client);
        if (pendingTimer) {
            clock.clearTimeout(pendingTimer);
            monitorSendTimers.delete(client);
        }

        const monitorMap = clientMonitorState.get(client) || new Map();
        const data = {};

        for (const [id, entry] of monitorMap.entries()) {
            data[id] = {
                path: entry.path,
                history: entry.history,
                error: entry.error || null,
                staticEnabled: !!entry.staticEnabled,
                staticError: entry.staticError || (entry.staticEnabled ? entry.error : null) || null,
            };
        }

        sendState(client, data);
    }

    function queueMonitorStateSend(client) {
        if (monitorSendTimers.has(client)) return;

        monitorSendTimers.set(
            client,
            clock.setTimeout(() => {
                monitorSendTimers.delete(client);
                sendMonitorStateToClient(client);
            }, MONITOR_SEND_COALESCE_MS)
        );
    }

    async function seedClientCurrentValue(client, id, path, entry) {
        if (!gameBridge) return;

        try {
            const result = await gameBridge.callHelper("window.readGamePath", [path]);

            if (result.error) {
                return;
            }

            const payload = result.value;
            if (!payload || !Object.prototype.hasOwnProperty.call(payload, "value")) {
                return;
            }

            if (clientMonitorState.get(client)?.get(id) !== entry) return;

            pushHistory(entry, payload.value);
        } catch (err) {
            log.debug(`Could not seed monitor value for ${path}: ${err.message}`);
        }
    }

    async function releaseGlobalWatcher(path) {
        const watcher = globalWatchersByPath.get(path);
        if (!watcher) return;

        if (countMonitorSubscribers(watcher.id) > 0) return;

        if (watcher.releasePromise) {
            await watcher.releasePromise;
            return;
        }

        watcher.releasePromise = (async () => {
            if (gameBridge) {
                try {
                    const result = await gameBridge.callHelper("window.monitorUnwrap", [watcher.id]);
                    if (result.error) {
                        log.error(`Error unsubscribing monitor ${watcher.id}:`, result.error.text);
                    }
                } catch (err) {
                    log.error(`Error unsubscribing monitor ${watcher.id}:`, err.message);
                }
            }

            // Only remove our own entry: rewrapAllWatchers may have replaced it
            // with a fresh watcher while the unwrap was in flight. A failed unwrap
            // still drops bookkeeping — a later subscribe re-wraps and the game
            // returns "Already watching this ID", which the subscribe path accepts.
            if (globalWatchersByPath.get(path) === watcher) {
                globalWatchersByPath.delete(path);
            }
        })();

        await watcher.releasePromise;
    }

    async function syncGlobalStaticState(path) {
        const watcher = globalWatchersByPath.get(path);
        if (!watcher || !gameBridge) return;
        if (watcher.staticSyncPromise) {
            await watcher.staticSyncPromise;
            return syncGlobalStaticState(path);
        }

        const shouldFreeze = countStaticSubscribers(watcher.id) > 0;
        if (watcher.staticEnabled === shouldFreeze) return;

        watcher.staticSyncPromise = (async () => {
            const helper = shouldFreeze ? "window.monitorFreeze" : "window.monitorUnfreeze";
            const result = await gameBridge.callHelper(helper, [watcher.id]);
            if (result.error) throw new Error(result.error.text || "Could not change static value state");
            const payload = result.value;
            if (!payload?.success) throw new Error(payload?.error || "Could not change static value state");
            watcher.staticEnabled = shouldFreeze;
            watcher.staticError = null;
        })();

        try {
            await watcher.staticSyncPromise;
        } catch (err) {
            watcher.staticError = err.message;
            log.error(`Could not update static value for ${path}: ${err.message}`);
            throw err;
        } finally {
            watcher.staticSyncPromise = null;
        }
    }

    async function cleanupClientSubscriptions(client) {
        clearAllMonitorSubscribeRetries(client);

        const sendTimer = monitorSendTimers.get(client);
        if (sendTimer) {
            clock.clearTimeout(sendTimer);
            monitorSendTimers.delete(client);
        }

        const monitorMap = clientMonitorState.get(client);
        if (!monitorMap) return;

        clientMonitorState.delete(client);

        const paths = new Set();
        for (const entry of monitorMap.values()) {
            paths.add(entry.path);
        }

        for (const path of paths) {
            try {
                await syncGlobalStaticState(path);
            } catch {
                // Unwrapping the last watcher still restores the current property value.
            }
            await releaseGlobalWatcher(path);
        }
    }

    /**
     * Handles value updates from the game.
     * Updates only clients subscribed to the specific monitor id.
     * @param {Object} msg
     */
    function handleMonitorUpdate(msg) {
        const { id, value } = msg;
        const ts = typeof msg.ts === "number" ? msg.ts : clock.now();
        if (!id) return;

        // Routed purely by client subscription: entries exist before the wrap
        // evaluate resolves, so wrap()'s immediate initial broadcast is captured.
        for (const [client, monitorMap] of clientMonitorState.entries()) {
            const entry = monitorMap.get(id);
            if (!entry) continue;

            delete entry.error;
            pushHistory(entry, value, ts);
            queueMonitorStateSend(client);
        }
    }

    /**
     * The game reconnected with a fresh JS context: every previously installed
     * wrap is gone. Drop the stale bookkeeping and resubscribe all current client
     * subscriptions through the normal flow (its transient retries cover a game
     * that is still booting).
     */
    function rewrapAllWatchers() {
        globalWatchersByPath.clear();

        for (const [client, monitorMap] of clientMonitorState.entries()) {
            for (const entry of monitorMap.values()) {
                void handleMonitorSubscribe(client, entry.path, entry.staticEnabled, 0, true);
            }
        }
    }

    /**
     * Ensures a game-side wrap exists for the path.
     * @param {string} path
     * @returns {Promise<{ ok: true, value?: any } | { ok: false, error: string, transient: boolean }>}
     *   `value` is only present when this call installed the wrap.
     */
    async function ensureGlobalWatcher(path) {
        const id = monitorIdFromPath(path);

        let watcher = globalWatchersByPath.get(path);
        if (watcher?.releasePromise) {
            await watcher.releasePromise;
            watcher = globalWatchersByPath.get(path);
        }
        if (watcher) return { ok: true };

        const result = await gameBridge.callHelper("window.monitorWrap", [id, path]);

        if (result.error) {
            const errorText = result.error.text;
            return { ok: false, error: errorText, transient: isTransientMonitorSubscribeError(errorText) };
        }

        const payload = result.value;
        if (payload && payload.success) {
            globalWatchersByPath.set(path, { id, staticEnabled: false });
            return { ok: true, value: payload.value };
        }
        if (payload && payload.error === "Already watching this ID") {
            globalWatchersByPath.set(path, { id, staticEnabled: false });
            return { ok: true };
        }

        const errorText = (payload && payload.error) || "monitorWrap failed";
        return { ok: false, error: errorText, transient: isTransientMonitorSubscribeError(errorText) };
    }

    /**
     * Handles subscription requests from a specific UI client.
     * Uses per-client monitor lists and shared global runtime hooks.
     * @param {object} client
     * @param {string} path
     * @param {boolean} [staticEnabled]
     * @param {number} [retryAttempt]
     * @param {boolean} [forceAttempt] - Re-run the wrap even if a client entry already exists
     */
    async function handleMonitorSubscribe(client, path, staticEnabled = false, retryAttempt = 0, forceAttempt = false) {
        if (!gameBridge) return;
        if (typeof path !== "string" || !path.trim()) return;

        const normalizedPath = path.trim();
        const id = monitorIdFromPath(normalizedPath);
        const monitorMap = getClientMonitorMap(client);
        const existing = monitorMap.get(id);

        if (existing && !existing.error && existing.staticEnabled === staticEnabled && !forceAttempt) {
            sendMonitorStateToClient(client);
            return;
        }

        // Optimistic entry (history preserved on rewrap): it also catches wrap()'s
        // initial broadcast, which can arrive before the evaluate below resolves.
        const entry = { path: normalizedPath, history: existing ? existing.history : [], staticEnabled };
        monitorMap.set(id, entry);

        try {
            const result = await ensureGlobalWatcher(normalizedPath);

            const current = clientMonitorState.get(client)?.get(id);
            if (current !== entry) {
                // A late result belongs to the old attempt, including its errors and retries.
                if (!current && result.ok) await releaseGlobalWatcher(normalizedPath);
                return;
            }

            if (!result.ok) {
                if (result.transient) {
                    log.debug(`Monitor subscribe pending for ${id}: ${result.error}`);
                    scheduleMonitorSubscribeRetry(client, normalizedPath, id, retryAttempt);
                    return;
                }
                failMonitorSubscription(client, id, result.error);
                return;
            }

            clearMonitorSubscribeRetry(client, id);

            if (entry.history.length === 0) {
                if (Object.prototype.hasOwnProperty.call(result, "value")) {
                    pushHistory(entry, result.value);
                } else {
                    await seedClientCurrentValue(client, id, normalizedPath, entry);
                    if (clientMonitorState.get(client)?.get(id) !== entry) return;
                }
            }
            try {
                await syncGlobalStaticState(normalizedPath);
            } catch (err) {
                entry.staticError = err.message;
            }
            sendMonitorStateToClient(client);
        } catch (err) {
            if (clientMonitorState.get(client)?.get(id) !== entry) return;
            if (isTransientMonitorSubscribeError(err.message)) {
                log.debug(`Monitor subscribe pending for ${id}: ${err.message}`);
                scheduleMonitorSubscribeRetry(client, normalizedPath, id, retryAttempt);
                return;
            }
            failMonitorSubscription(client, id, err.message);
        }
    }

    /**
     * Handles unsubscription requests from a specific UI client.
     * @param {object} client
     * @param {string} id
     * @param {string} path
     */
    async function handleMonitorUnsubscribe(client, id, path) {
        const monitorMap = clientMonitorState.get(client);
        if (!monitorMap) return;

        let targetId = null;

        if (typeof id === "string" && id && monitorMap.has(id)) {
            targetId = id;
        }

        if (!targetId && typeof path === "string" && path.trim()) {
            const pathId = monitorIdFromPath(path.trim());
            if (monitorMap.has(pathId)) {
                targetId = pathId;
            }
        }

        if (!targetId) {
            return;
        }

        const entry = monitorMap.get(targetId);
        monitorMap.delete(targetId);
        clearMonitorSubscribeRetry(client, targetId);
        sendMonitorStateToClient(client);

        if (entry && entry.path) {
            try {
                await syncGlobalStaticState(entry.path);
            } catch {
                // Releasing the last watcher restores the current property value.
            }
            await releaseGlobalWatcher(entry.path);
        }
    }

    function close() {
        // Best-effort, non-blocking: awaiting a CDP evaluate during shutdown can
        // hang if the connection is already gone. We still inspect
        // game errors so a context failure is logged rather than lost.
        if (gameBridge) {
            void (async () => {
                try {
                    const result = await gameBridge.callHelper("window.monitorUnwrapAll");
                    if (result.error) {
                        log.error("Error unwrapping all monitors during shutdown:", result.error.text);
                    }
                } catch (err) {
                    log.error("Error unwrapping all monitors during shutdown:", err.message);
                }
            })();
        }

        clientMonitorState.clear();
        for (const retryMap of monitorSubscribeRetryTimers.values()) {
            for (const timer of retryMap.values()) {
                clock.clearTimeout(timer);
            }
        }
        monitorSubscribeRetryTimers.clear();
        for (const timer of monitorSendTimers.values()) {
            clock.clearTimeout(timer);
        }
        monitorSendTimers.clear();
        globalWatchersByPath.clear();
    }

    return {
        sendState: sendMonitorStateToClient,
        subscribe: (client, path, staticEnabled = false) => handleMonitorSubscribe(client, path, staticEnabled),
        unsubscribe: handleMonitorUnsubscribe,
        disconnect: cleanupClientSubscriptions,
        update: handleMonitorUpdate,
        reconnect: rewrapAllWatchers,
        close,
    };
}

module.exports = { createMonitor };
