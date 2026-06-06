import { useCallback, useContext, useEffect, useRef, useState } from 'react';
import { Loader2, RefreshCw, Youtube } from 'lucide-react';
// @ts-ignore - TS module resolution issue with shared exports in Vercel tsc build; exports exist at runtime
import { AuthContext, resolveStudioBackendUrl, startYouTubeBrowserConnect, studioAgentOAuthReturnUrl } from '../../shared.tsx';

type ChannelRow = {
    channel_id: string;
    title: string;
    channel_handle?: string;
};

export default function AgentYouTubeConnect({
    onChannelsLoaded,
}: {
    onChannelsLoaded?: (channels: ChannelRow[]) => void;
}) {
    const { session } = useContext(AuthContext);
    const accessToken = session?.access_token ?? '';
    const [channels, setChannels] = useState<ChannelRow[]>([]);
    const [loading, setLoading] = useState(false);
    const [connecting, setConnecting] = useState(false);
    const [banner, setBanner] = useState('');
    const [error, setError] = useState('');
    const loadInFlightRef = useRef(false);
    const onChannelsLoadedRef = useRef(onChannelsLoaded);
    onChannelsLoadedRef.current = onChannelsLoaded;

    const loadChannels = useCallback(
        async (opts?: { sync?: boolean }) => {
            if (!accessToken || loadInFlightRef.current) return;
            loadInFlightRef.current = true;
            setLoading(true);
            setError('');
            try {
                const sync = opts?.sync ? 'true' : 'false';
                const res = await fetch(resolveStudioBackendUrl(`/api/youtube/channels?sync=${sync}`), {
                    headers: { Authorization: `Bearer ${accessToken}` },
                });
                const data = (await res.json().catch(() => ({}))) as {
                    channels?: ChannelRow[];
                    detail?: string;
                };
                if (!res.ok) throw new Error(String(data.detail || `HTTP ${res.status}`));
                const list = Array.isArray(data.channels) ? data.channels : [];
                setChannels(list);
                onChannelsLoadedRef.current?.(list);
            } catch (e) {
                const msg = (e as Error).message || '';
                if (/429|too many|queue/i.test(msg)) {
                    setError(
                        'YouTube refresh is waiting on API capacity (RunPod queue). '
                        + 'Your agent session on Fly is separate — you can still approve renders. '
                        + 'Retry refresh in a minute.',
                    );
                } else {
                    setError(msg);
                }
                if (!channels.length) {
                    onChannelsLoadedRef.current?.([]);
                }
            } finally {
                setLoading(false);
                loadInFlightRef.current = false;
            }
        },
        [accessToken],
    );

    useEffect(() => {
        if (!accessToken) {
            setChannels([]);
            return;
        }
        let syncAfterOAuth = false;
        try {
            const params = new URLSearchParams(window.location.search);
            const yt = params.get('youtube');
            if (yt === 'connected') {
                setBanner('YouTube connected — the agent can read your channel analytics.');
                syncAfterOAuth = true;
                params.delete('youtube');
                params.delete('youtube_message');
                const u = new URL(window.location.href);
                u.search = params.toString();
                window.history.replaceState({}, '', u.toString());
            } else if (yt === 'error') {
                const msg = params.get('youtube_message') || 'YouTube connection failed';
                setError(decodeURIComponent(msg.replace(/\+/g, ' ')));
                params.delete('youtube');
                params.delete('youtube_message');
                const u = new URL(window.location.href);
                u.search = params.toString();
                window.history.replaceState({}, '', u.toString());
            }
        } catch {
            /* ignore */
        }
        void loadChannels({ sync: syncAfterOAuth });
    }, [accessToken, loadChannels]);

    const connect = () => {
        if (!accessToken) return;
        setConnecting(true);
        setError('');
        try {
            startYouTubeBrowserConnect(accessToken, studioAgentOAuthReturnUrl());
        } catch (e) {
            setError((e as Error).message);
            setConnecting(false);
        }
    };

    if (!accessToken) return null;

    const connected = channels.length > 0;
    const primary = channels[0];

    return (
        <div
            className={`shrink-0 rounded-xl border px-3 py-2.5 ${
                connected
                    ? 'border-red-500/20 bg-red-500/5'
                    : 'border-amber-500/25 bg-amber-500/5'
            }`}
        >
            <div className="flex flex-wrap items-center gap-2">
                <Youtube className={`h-4 w-4 shrink-0 ${connected ? 'text-red-300' : 'text-amber-300'}`} />
                {connected ? (
                    <div className="min-w-0 flex-1">
                        <p className="text-xs font-semibold text-white">{primary.title}</p>
                        {primary.channel_handle && (
                            <p className="text-[10px] text-gray-400">{primary.channel_handle}</p>
                        )}
                        {channels.length > 1 && (
                            <p className="text-[10px] text-gray-500">
                                +{channels.length - 1} more channel{channels.length > 2 ? 's' : ''}
                            </p>
                        )}
                    </div>
                ) : (
                    <p className="min-w-0 flex-1 text-xs text-amber-100/90">
                        Connect YouTube so the agent can recommend topics and read your analytics.
                    </p>
                )}
                <button
                    type="button"
                    onClick={() => void loadChannels({ sync: true })}
                    disabled={loading}
                    className="rounded-lg border border-white/10 p-1.5 text-gray-400 transition hover:text-white disabled:opacity-50"
                    title="Refresh channels"
                >
                    {loading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
                </button>
                <button
                    type="button"
                    onClick={connect}
                    disabled={connecting}
                    className="rounded-lg bg-red-600 px-3 py-1.5 text-[11px] font-semibold text-white transition hover:bg-red-500 disabled:opacity-60"
                >
                    {connecting ? 'Opening Google…' : connected ? 'Add channel' : 'Connect YouTube'}
                </button>
            </div>
            {banner && <p className="mt-2 text-[10px] text-emerald-300/90">{banner}</p>}
            {error && <p className="mt-2 text-[10px] text-red-300/90">{error}</p>}
        </div>
    );
}
