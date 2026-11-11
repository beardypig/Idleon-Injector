/** Own frontend Monitor subscriptions; paths are raw Search result paths. */
export function createLiveMonitor({ getRawMonitors, subscribe: sendSubscribe, unsubscribe: sendUnsubscribe }) {
    const subscribedPaths = new Map();
    const idForPath = (path) => "mon:" + encodeURIComponent("gga." + path);

    const subscribe = (path, staticEnabled = false) => {
        if (subscribedPaths.get(path) === staticEnabled) return;
        sendSubscribe(idForPath(path), "gga." + path, staticEnabled);
        subscribedPaths.set(path, staticEnabled);
    };

    const unsubscribe = (path) => {
        if (!subscribedPaths.has(path)) return;
        sendUnsubscribe(idForPath(path));
        subscribedPaths.delete(path);
    };

    return {
        subscribe,
        unsubscribe,
        get: (path) => getRawMonitors()[idForPath(path)] || null,
        reconcile(paths, staticPaths = []) {
            const staticSet = new Set(staticPaths);
            const desiredPaths = new Map(paths.map((path) => [path, staticSet.has(path)]));
            const rawMonitors = getRawMonitors();

            for (const [path, staticEnabled] of desiredPaths) {
                // Preserve Search's retry when a previously requested runtime entry disappears.
                if (subscribedPaths.has(path) && !rawMonitors[idForPath(path)]) subscribedPaths.delete(path);
                if (!subscribedPaths.has(path) || subscribedPaths.get(path) !== staticEnabled)
                    subscribe(path, staticEnabled);
            }

            for (const path of [...subscribedPaths.keys()]) {
                if (!desiredPaths.has(path)) unsubscribe(path);
            }
        },
    };
}
