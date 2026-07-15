/** Cross-panel production model preference (Agent, Long-form, Create). */
export const STUDIO_IMAGE_MODEL_PREF_KEY = 'studio_production_image_model';
export const DEFAULT_IMAGE_MODEL = 'ernie_image';

export function loadImageModelPref(fallback = DEFAULT_IMAGE_MODEL): string {
    try {
        const raw = localStorage.getItem(STUDIO_IMAGE_MODEL_PREF_KEY);
        return String(raw || '').trim() || fallback;
    } catch {
        return fallback;
    }
}

export function saveImageModelPref(id: string): void {
    try {
        const normalized = String(id || '').trim();
        if (!normalized) return;
        localStorage.setItem(STUDIO_IMAGE_MODEL_PREF_KEY, normalized);
    } catch {
        /* ignore quota / private mode */
    }
}
