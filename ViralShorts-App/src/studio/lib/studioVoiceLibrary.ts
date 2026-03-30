export type StudioVoicePreset = {
    id: string;
    name: string;
    profile: string;
    source: string;
    available: boolean;
    defaultSpeed: number;
    defaultPitch: number;
};

export const activeCustomVoices: StudioVoicePreset[] = [
    { id: 'studio_voice_core', name: 'Core Neutral', profile: 'Balanced all-purpose narrator', source: 'Local owned variant', available: true, defaultSpeed: 1.0, defaultPitch: 1.0 },
    { id: 'studio_voice_hook', name: 'Hook Sprint', profile: 'Fast short-form opener energy', source: 'Local owned variant', available: true, defaultSpeed: 1.12, defaultPitch: 1.03 },
    { id: 'studio_voice_drama', name: 'Dark Drama', profile: 'Lower, heavier dramatic delivery', source: 'Local owned variant', available: true, defaultSpeed: 0.96, defaultPitch: 0.94 },
    { id: 'studio_voice_confession', name: 'Relatable Confession', profile: 'Confessional story tone', source: 'Local owned variant', available: true, defaultSpeed: 1.04, defaultPitch: 0.98 },
    { id: 'studio_voice_founder', name: 'Founder Calm', profile: 'Controlled operator/founder narration', source: 'Local owned variant', available: true, defaultSpeed: 0.98, defaultPitch: 0.97 },
    { id: 'studio_voice_punch', name: 'Viral Punch', profile: 'Sharper payoff emphasis', source: 'Local owned variant', available: true, defaultSpeed: 1.1, defaultPitch: 1.02 },
    { id: 'studio_voice_doc', name: 'Documentary Steel', profile: 'Clean explainer authority', source: 'Local owned variant', available: true, defaultSpeed: 0.97, defaultPitch: 0.95 },
    { id: 'studio_voice_luxe', name: 'Luxury Ad', profile: 'Premium polished ad cadence', source: 'Local owned variant', available: true, defaultSpeed: 1.01, defaultPitch: 1.01 },
    { id: 'studio_voice_story', name: 'Storyteller Warm', profile: 'Warm cinematic storytelling', source: 'Local owned variant', available: true, defaultSpeed: 0.99, defaultPitch: 1.04 },
    { id: 'studio_voice_intense', name: 'Intense Clarity', profile: 'Sharper urgency for conflict beats', source: 'Local owned variant', available: true, defaultSpeed: 1.08, defaultPitch: 0.97 },
    { id: 'studio_voice_genz', name: 'Gen Z Hook', profile: 'Modern social pacing', source: 'Local owned variant', available: true, defaultSpeed: 1.14, defaultPitch: 1.05 },
    { id: 'studio_voice_motive', name: 'Motivation Rise', profile: 'Slightly elevated inspirational tone', source: 'Local owned variant', available: true, defaultSpeed: 1.03, defaultPitch: 1.06 },
    { id: 'studio_voice_noir', name: 'Noir Tension', profile: 'Low-key suspense delivery', source: 'Local owned variant', available: true, defaultSpeed: 0.95, defaultPitch: 0.92 },
];

export const customVoiceLibrary: StudioVoicePreset[] = [
    ...activeCustomVoices,
    ...Array.from({ length: 37 }, (_, index) => ({
        id: `studio_voice_reserved_${String(index + 1).padStart(2, '0')}`,
        name: `Reserved Slot ${String(index + 14).padStart(2, '0')}`,
        profile: 'Reserved for future licensed or user-owned voice packs',
        source: 'Reserved voice slot',
        available: false,
        defaultSpeed: 1,
        defaultPitch: 1,
    })),
];

export const customVoicePresetMap = new Map(customVoiceLibrary.map((voice) => [voice.id, voice]));
