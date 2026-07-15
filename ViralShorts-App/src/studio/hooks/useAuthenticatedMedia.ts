import { useEffect, useMemo, useState } from 'react';

import {
    fetchMediaAsset,
    isProtectedStudioMedia,
    resolveMediaAssetUrl,
} from '../lib/agentProduction';

export type AuthenticatedMediaState = {
    urls: string[];
    loading: boolean;
    error: string;
};

/**
 * Resolve private Studio media through a Bearer-authenticated fetch and expose
 * short-lived object URLs to native img/video/model-viewer elements. Public
 * third-party assets remain direct URLs and never receive the Studio JWT.
 */
export function useAuthenticatedMediaUrls(
    paths: Array<string | null | undefined>,
    accessToken: string,
    enabled = true,
): AuthenticatedMediaState {
    const pathKey = JSON.stringify(paths.map((path) => String(path || '').trim()));
    const normalizedPaths = useMemo(() => JSON.parse(pathKey) as string[], [pathKey]);
    const [state, setState] = useState<AuthenticatedMediaState>({
        urls: normalizedPaths.map(() => ''),
        loading: false,
        error: '',
    });

    useEffect(() => {
        let cancelled = false;
        const controller = new AbortController();
        const objectUrls: string[] = [];
        let firstError = '';

        if (!enabled || normalizedPaths.every((path) => !path)) {
            setState({ urls: normalizedPaths.map(() => ''), loading: false, error: '' });
            return () => controller.abort();
        }

        setState({ urls: normalizedPaths.map(() => ''), loading: true, error: '' });
        void Promise.all(normalizedPaths.map(async (path) => {
            try {
                if (!path) return '';
                const resolved = resolveMediaAssetUrl(path);
                if (!resolved) throw new Error('Invalid media URL');
                if (!isProtectedStudioMedia(path)) return resolved;
                const res = await fetchMediaAsset(path, accessToken, {
                    cache: 'no-store',
                    signal: controller.signal,
                });
                if (!res.ok) throw new Error(`Media request failed (${res.status})`);
                const objectUrl = URL.createObjectURL(await res.blob());
                objectUrls.push(objectUrl);
                return objectUrl;
            } catch (error: unknown) {
                if (controller.signal.aborted) throw error;
                firstError ||= error instanceof Error ? error.message : 'Media request failed';
                return '';
            }
        })).then((urls) => {
            if (!cancelled) setState({ urls, loading: false, error: firstError });
        }).catch((error: unknown) => {
            if (cancelled || controller.signal.aborted) return;
            setState({
                urls: normalizedPaths.map(() => ''),
                loading: false,
                error: error instanceof Error ? error.message : 'Media request failed',
            });
        });

        return () => {
            cancelled = true;
            controller.abort();
            objectUrls.forEach((url) => URL.revokeObjectURL(url));
        };
    }, [accessToken, enabled, normalizedPaths]);

    return state;
}

export function useAuthenticatedMediaUrl(
    path: string | null | undefined,
    accessToken: string,
    enabled = true,
) {
    const state = useAuthenticatedMediaUrls([path], accessToken, enabled);
    return { ...state, url: state.urls[0] || '' };
}
