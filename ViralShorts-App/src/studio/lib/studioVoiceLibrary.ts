export type StudioVoicePreset = {
    id: string;
    name: string;
    profile: string;
    source: string;
    available: boolean;
    defaultSpeed: number;
    defaultPitch: number;
    category?: string;
    gender?: string;
    accent?: string;
    languageFocus?: string;
    backingVoiceId?: string;
};

const defaultElevenVoiceIds = {
    sarah: 'EXAVITQu4vr4xnSDxMaL',
    laura: 'FGY2WhTYpPnrIDTdsKH5',
    charlotte: 'XB0fDUnXU5powFXDhCwa',
    adam: 'pNInz6obpgDQGcFmaJgB',
    daniel: 'onwK4e9ZLuTAKqWW03F9',
} as const;

const tunedVoice = (
    id: string,
    name: string,
    profile: string,
    category: string,
    backingVoiceId: string,
    options: Partial<StudioVoicePreset> = {},
): StudioVoicePreset => ({
    id,
    name,
    profile,
    source: 'Catalyst tuned profile',
    available: true,
    defaultSpeed: options.defaultSpeed ?? 1,
    defaultPitch: options.defaultPitch ?? 1,
    category,
    gender: options.gender,
    accent: options.accent,
    languageFocus: options.languageFocus ?? 'global english',
    backingVoiceId,
});

export const activeCustomVoices: StudioVoicePreset[] = [
    tunedVoice('studio_voice_core', 'Core Neutral', 'Balanced all-purpose narrator', 'Core', defaultElevenVoiceIds.laura, { gender: 'female', accent: 'american' }),
    tunedVoice('studio_voice_hook', 'Hook Sprint', 'Fast short-form opener energy', 'Hooks', defaultElevenVoiceIds.adam, { defaultSpeed: 1.12, defaultPitch: 1.03, gender: 'male', accent: 'american' }),
    tunedVoice('studio_voice_drama', 'Dark Drama', 'Lower, heavier dramatic delivery', 'Cinema', defaultElevenVoiceIds.daniel, { defaultSpeed: 0.96, defaultPitch: 0.94, gender: 'male', accent: 'british' }),
    tunedVoice('studio_voice_confession', 'Relatable Confession', 'Confessional story tone', 'Story', defaultElevenVoiceIds.charlotte, { defaultSpeed: 1.04, defaultPitch: 0.98, gender: 'female', accent: 'british' }),
    tunedVoice('studio_voice_founder', 'Founder Calm', 'Controlled operator/founder narration', 'Authority', defaultElevenVoiceIds.adam, { defaultSpeed: 0.98, defaultPitch: 0.97, gender: 'male', accent: 'american' }),
    tunedVoice('studio_voice_punch', 'Viral Punch', 'Sharper payoff emphasis', 'Hooks', defaultElevenVoiceIds.adam, { defaultSpeed: 1.1, defaultPitch: 1.02, gender: 'male', accent: 'american' }),
    tunedVoice('studio_voice_doc', 'Documentary Steel', 'Clean explainer authority', 'Authority', defaultElevenVoiceIds.daniel, { defaultSpeed: 0.97, defaultPitch: 0.95, gender: 'male', accent: 'british' }),
    tunedVoice('studio_voice_luxe', 'Luxury Ad', 'Premium polished ad cadence', 'Commercial', defaultElevenVoiceIds.laura, { defaultSpeed: 1.01, defaultPitch: 1.01, gender: 'female', accent: 'american' }),
    tunedVoice('studio_voice_story', 'Storyteller Warm', 'Warm cinematic storytelling', 'Story', defaultElevenVoiceIds.charlotte, { defaultSpeed: 0.99, defaultPitch: 1.04, gender: 'female', accent: 'british' }),
    tunedVoice('studio_voice_intense', 'Intense Clarity', 'Sharper urgency for conflict beats', 'Cinema', defaultElevenVoiceIds.daniel, { defaultSpeed: 1.08, defaultPitch: 0.97, gender: 'male', accent: 'british' }),
    tunedVoice('studio_voice_genz', 'Gen Z Hook', 'Modern social pacing', 'Social', defaultElevenVoiceIds.sarah, { defaultSpeed: 1.14, defaultPitch: 1.05, gender: 'female', accent: 'american' }),
    tunedVoice('studio_voice_motive', 'Motivation Rise', 'Slightly elevated inspirational tone', 'Motivation', defaultElevenVoiceIds.sarah, { defaultSpeed: 1.03, defaultPitch: 1.06, gender: 'female', accent: 'american' }),
    tunedVoice('studio_voice_noir', 'Noir Tension', 'Low-key suspense delivery', 'Cinema', defaultElevenVoiceIds.daniel, { defaultSpeed: 0.95, defaultPitch: 0.92, gender: 'male', accent: 'british' }),
];

const expansionVoices: StudioVoicePreset[] = [
    tunedVoice('studio_voice_anchor', 'Anchor Prime', 'Broadcast opener with cool authority', 'Authority', defaultElevenVoiceIds.adam, { defaultSpeed: 0.99, defaultPitch: 0.97, gender: 'male', accent: 'american' }),
    tunedVoice('studio_voice_boardroom', 'Boardroom Pulse', 'Confident executive explainer tone', 'Authority', defaultElevenVoiceIds.adam, { gender: 'male', accent: 'american' }),
    tunedVoice('studio_voice_moneyline', 'Moneyline Sharp', 'Finance niche narration with edge', 'Finance', defaultElevenVoiceIds.daniel, { defaultSpeed: 1.04, defaultPitch: 0.96, gender: 'male', accent: 'british' }),
    tunedVoice('studio_voice_mentor', 'Mentor Calm', 'Steady educational delivery', 'Authority', defaultElevenVoiceIds.laura, { defaultSpeed: 0.98, defaultPitch: 0.99, gender: 'female', accent: 'american' }),
    tunedVoice('studio_voice_oracle', 'Oracle Velvet', 'Measured premium female authority', 'Authority', defaultElevenVoiceIds.charlotte, { defaultSpeed: 0.96, defaultPitch: 1.01, gender: 'female', accent: 'british' }),
    tunedVoice('studio_voice_casefile', 'Casefile Low', 'Investigative true-story depth', 'Cinema', defaultElevenVoiceIds.daniel, { defaultSpeed: 0.95, defaultPitch: 0.93, gender: 'male', accent: 'british' }),
    tunedVoice('studio_voice_vault', 'Vault Whisper', 'Suspense-led quiet intensity', 'Cinema', defaultElevenVoiceIds.laura, { defaultSpeed: 0.92, defaultPitch: 0.95, gender: 'female', accent: 'american' }),
    tunedVoice('studio_voice_titan', 'Titan Trailer', 'Big theatrical trailer cadence', 'Cinema', defaultElevenVoiceIds.adam, { defaultSpeed: 1.03, defaultPitch: 0.91, gender: 'male', accent: 'american' }),
    tunedVoice('studio_voice_ember', 'Ember Confide', 'Soft female confession with warmth', 'Story', defaultElevenVoiceIds.charlotte, { defaultSpeed: 1.02, defaultPitch: 1.04, gender: 'female', accent: 'british' }),
    tunedVoice('studio_voice_hearth', 'Hearth Journal', 'Diary-like intimate storytelling', 'Story', defaultElevenVoiceIds.laura, { defaultSpeed: 1.01, defaultPitch: 1.02, gender: 'female', accent: 'american' }),
    tunedVoice('studio_voice_cornerbooth', 'Corner Booth', 'Conversation-driven male storyteller', 'Story', defaultElevenVoiceIds.adam, { gender: 'male', accent: 'american' }),
    tunedVoice('studio_voice_legacy', 'Legacy Warmth', 'Reflective documentary memoir feel', 'Story', defaultElevenVoiceIds.daniel, { defaultSpeed: 0.97, defaultPitch: 1.01, gender: 'male', accent: 'british' }),
    tunedVoice('studio_voice_fuse', 'Fuse Cut', 'Snappy social pacing for modern hooks', 'Social', defaultElevenVoiceIds.sarah, { defaultSpeed: 1.16, defaultPitch: 1.02, gender: 'female', accent: 'american' }),
    tunedVoice('studio_voice_scrollstop', 'Scroll Stop', 'Aggressive short-form attention grabber', 'Social', defaultElevenVoiceIds.adam, { defaultSpeed: 1.18, defaultPitch: 1.03, gender: 'male', accent: 'american' }),
    tunedVoice('studio_voice_reelcut', 'Reel Cut', 'Punchy creator-economy narration', 'Social', defaultElevenVoiceIds.sarah, { defaultSpeed: 1.13, defaultPitch: 1.05, gender: 'female', accent: 'american' }),
    tunedVoice('studio_voice_afterparty', 'Afterparty', 'Playful social confidence with bounce', 'Social', defaultElevenVoiceIds.sarah, { defaultSpeed: 1.09, defaultPitch: 1.07, gender: 'female', accent: 'american' }),
    tunedVoice('studio_voice_marble', 'Marble Luxe', 'Fashion-ad polish with soft precision', 'Commercial', defaultElevenVoiceIds.laura, { defaultPitch: 1.03, gender: 'female', accent: 'american' }),
    tunedVoice('studio_voice_slate', 'Slate Premium', 'Cool restrained premium promo tone', 'Commercial', defaultElevenVoiceIds.daniel, { defaultSpeed: 0.99, defaultPitch: 0.98, gender: 'male', accent: 'british' }),
    tunedVoice('studio_voice_gilded', 'Gilded Sell', 'Upscale offer presentation without hype', 'Commercial', defaultElevenVoiceIds.charlotte, { defaultSpeed: 1.02, gender: 'female', accent: 'british' }),
    tunedVoice('studio_voice_launch', 'Launch Day', 'Sharp product launch pacing', 'Commercial', defaultElevenVoiceIds.adam, { defaultSpeed: 1.06, defaultPitch: 1.01, gender: 'male', accent: 'american' }),
    tunedVoice('studio_voice_zenith', 'Zenith Pitch', 'Clean premium female sales narration', 'Commercial', defaultElevenVoiceIds.sarah, { defaultSpeed: 1.03, defaultPitch: 1.05, gender: 'female', accent: 'american' }),
    tunedVoice('studio_voice_steelcase', 'Steelcase', 'Boardroom explainer for business channels', 'Business', defaultElevenVoiceIds.adam, { defaultSpeed: 0.99, defaultPitch: 0.96, gender: 'male', accent: 'american' }),
    tunedVoice('studio_voice_operator', 'Operator Loop', 'Analytical founder/operator breakdowns', 'Business', defaultElevenVoiceIds.daniel, { defaultSpeed: 1.01, defaultPitch: 0.97, gender: 'male', accent: 'british' }),
    tunedVoice('studio_voice_blueprint', 'Blueprint', 'Startup systems and case-study delivery', 'Business', defaultElevenVoiceIds.laura, { gender: 'female', accent: 'american' }),
    tunedVoice('studio_voice_empire', 'Empire Rollup', 'Scale-up content with direct confidence', 'Business', defaultElevenVoiceIds.adam, { defaultSpeed: 1.04, defaultPitch: 0.98, gender: 'male', accent: 'american' }),
    tunedVoice('studio_voice_marketbell', 'Market Bell', 'Financial-news style with urgency', 'Finance', defaultElevenVoiceIds.daniel, { defaultSpeed: 1.05, defaultPitch: 0.95, gender: 'male', accent: 'british' }),
    tunedVoice('studio_voice_equity', 'Equity Calm', 'Measured market explainer voice', 'Finance', defaultElevenVoiceIds.laura, { defaultSpeed: 0.99, defaultPitch: 0.99, gender: 'female', accent: 'american' }),
    tunedVoice('studio_voice_macro', 'Macro Desk', 'Macroeconomic breakdown with authority', 'Finance', defaultElevenVoiceIds.adam, { defaultSpeed: 0.98, defaultPitch: 0.96, gender: 'male', accent: 'american' }),
    tunedVoice('studio_voice_captable', 'Cap Table', 'Tech-business crossover narration', 'Finance', defaultElevenVoiceIds.charlotte, { defaultSpeed: 1.02, defaultPitch: 1.01, gender: 'female', accent: 'british' }),
    tunedVoice('studio_voice_quant', 'Quant Lens', 'Sharp analytical finance tone', 'Finance', defaultElevenVoiceIds.daniel, { defaultPitch: 0.94, gender: 'male', accent: 'british' }),
    tunedVoice('studio_voice_signal', 'Signal Wire', 'Clean AI/tech update delivery', 'Tech', defaultElevenVoiceIds.laura, { defaultSpeed: 1.04, gender: 'female', accent: 'american' }),
    tunedVoice('studio_voice_stack', 'Stack Driver', 'Builder-focused tech breakdown voice', 'Tech', defaultElevenVoiceIds.adam, { defaultSpeed: 1.02, defaultPitch: 0.98, gender: 'male', accent: 'american' }),
    tunedVoice('studio_voice_circuit', 'Circuit Cool', 'Precise future-tech narration', 'Tech', defaultElevenVoiceIds.charlotte, { defaultSpeed: 0.99, defaultPitch: 1.01, gender: 'female', accent: 'british' }),
    tunedVoice('studio_voice_kernel', 'Kernel Night', 'Dark tech and cyber commentary', 'Tech', defaultElevenVoiceIds.daniel, { defaultSpeed: 1.01, defaultPitch: 0.95, gender: 'male', accent: 'british' }),
    tunedVoice('studio_voice_patriot', 'Patriot Steel', 'Firm tactical documentary voice', 'Global', defaultElevenVoiceIds.adam, { defaultSpeed: 0.97, defaultPitch: 0.93, gender: 'male', accent: 'american' }),
    tunedVoice('studio_voice_crown', 'Crown Ledger', 'Refined UK authority profile', 'Global', defaultElevenVoiceIds.charlotte, { defaultSpeed: 0.98, defaultPitch: 0.99, gender: 'female', accent: 'british' }),
    tunedVoice('studio_voice_commonwealth', 'Commonwealth', 'Measured transatlantic-style delivery', 'Global', defaultElevenVoiceIds.daniel, { defaultPitch: 0.97, gender: 'male', accent: 'british' }),
    tunedVoice('studio_voice_globe', 'Globe Neutral', 'Neutral multilingual-friendly profile', 'Global', defaultElevenVoiceIds.laura, { gender: 'female', accent: 'american', languageFocus: 'multilingual friendly' }),
    tunedVoice('studio_voice_sunrise', 'Sunrise Clear', 'Bright international ad tone', 'Global', defaultElevenVoiceIds.sarah, { defaultSpeed: 1.05, defaultPitch: 1.06, gender: 'female', accent: 'american', languageFocus: 'multilingual friendly' }),
    tunedVoice('studio_voice_anthem', 'Anthem Lift', 'Inspirational global keynote voice', 'Motivation', defaultElevenVoiceIds.sarah, { defaultSpeed: 1.04, defaultPitch: 1.04, gender: 'female', accent: 'american', languageFocus: 'multilingual friendly' }),
    tunedVoice('studio_voice_resolve', 'Resolve Strong', 'Determined motivational narrator', 'Motivation', defaultElevenVoiceIds.adam, { defaultSpeed: 1.02, defaultPitch: 0.97, gender: 'male', accent: 'american' }),
    tunedVoice('studio_voice_ascent', 'Ascent Peak', 'Uplifting premium voiceover arc', 'Motivation', defaultElevenVoiceIds.charlotte, { defaultSpeed: 1.06, defaultPitch: 1.03, gender: 'female', accent: 'british' }),
    tunedVoice('studio_voice_victory', 'Victory Line', 'Sports-motivation crossover energy', 'Motivation', defaultElevenVoiceIds.adam, { defaultSpeed: 1.08, defaultPitch: 0.98, gender: 'male', accent: 'american' }),
    tunedVoice('studio_voice_lilt', 'Lilt Soft', 'Gentle feminine narration for soft beats', 'Story', defaultElevenVoiceIds.sarah, { defaultSpeed: 0.99, defaultPitch: 1.08, gender: 'female', accent: 'american' }),
    tunedVoice('studio_voice_quill', 'Quill Classic', 'Elegant literary narration', 'Story', defaultElevenVoiceIds.charlotte, { defaultSpeed: 0.97, defaultPitch: 1.02, gender: 'female', accent: 'british' }),
    tunedVoice('studio_voice_streetlight', 'Streetlight', 'Late-night urban story energy', 'Social', defaultElevenVoiceIds.adam, { defaultSpeed: 1.07, defaultPitch: 0.99, gender: 'male', accent: 'american' }),
    tunedVoice('studio_voice_afterhours', 'After Hours', 'Nocturnal mystery and intrigue', 'Cinema', defaultElevenVoiceIds.daniel, { defaultSpeed: 0.94, defaultPitch: 0.94, gender: 'male', accent: 'british' }),
];

export const customVoiceLibrary: StudioVoicePreset[] = [...activeCustomVoices, ...expansionVoices];
export const customVoicePresetMap = new Map(customVoiceLibrary.map((voice) => [voice.id, voice]));
export const customVoiceCategoryOptions = Array.from(
    new Set(customVoiceLibrary.map((voice) => voice.category).filter(Boolean) as string[]),
);
