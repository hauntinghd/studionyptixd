import type { StudioVoicePreset } from './studioVoiceLibrary';

export type ChatStoryStep = 'script' | 'theme' | 'background' | 'audio';
export type ChatStoryRole = 'sender' | 'receiver';
export type ChatStorySide = 'left' | 'right';

export type ChatStoryMessage = {
    id: string;
    role: ChatStoryRole;
    side: ChatStorySide;
    text: string;
};

export type ChatStoryTheme = {
    id: string;
    label: string;
    top: string;
    screen: string;
    incoming: string;
    outgoing: string;
    shell: string;
    previewShell: string;
};

export type ChatStoryBackground = {
    id: string;
    label: string;
    klass: string;
    accent: string;
    gradient: [string, string, string];
};

export type ChatStoryMusicOption = {
    id: string;
    label: string;
    src: string;
};

export type ChatStorySfxOption = {
    id: string;
    label: string;
    src: string;
};

export const chatStorySteps: ChatStoryStep[] = ['script', 'theme', 'background', 'audio'];

export const chatStoryThemes: ChatStoryTheme[] = [
    { id: 'dark', label: 'Dark Modern', top: 'bg-[#17171b] text-white', screen: 'bg-[#1c1c1f]', incoming: 'bg-[#2c2c30] text-white', outgoing: 'bg-[#2f7cff] text-white', shell: 'bg-[#121214] border-white/10', previewShell: 'bg-[#121214]' },
    { id: 'light', label: 'Light Clean', top: 'bg-white text-slate-900', screen: 'bg-[#f2f5f9]', incoming: 'bg-white text-slate-900', outgoing: 'bg-[#7cc4ff] text-slate-900', shell: 'bg-white border-gray-300/70', previewShell: 'bg-white' },
    { id: 'purple', label: 'Purple Pop', top: 'bg-[#1f1630] text-violet-50', screen: 'bg-[linear-gradient(180deg,#241533,#15131d)]', incoming: 'bg-[#2b2436] text-white', outgoing: 'bg-[#8b5cf6] text-white', shell: 'bg-[#191224] border-violet-500/30', previewShell: 'bg-[#191224]' },
];

export const chatStoryBackgrounds: ChatStoryBackground[] = [
    { id: 'subway', label: 'Subway Dash', klass: 'bg-[linear-gradient(160deg,#0f172a,#1d4ed8_40%,#22d3ee)]', accent: '#1d4ed8', gradient: ['#09111f', '#173885', '#35d6f8'] },
    { id: 'minecraft', label: 'Minecraft Parkour', klass: 'bg-[linear-gradient(160deg,#14532d,#4d7c0f_48%,#d9f99d)]', accent: '#4d7c0f', gradient: ['#102313', '#2f6c18', '#9cd46a'] },
    { id: 'cooking', label: 'Cooking Sizzle', klass: 'bg-[linear-gradient(160deg,#2c1610,#7c2d12_45%,#f59e0b)]', accent: '#f59e0b', gradient: ['#28140d', '#7b2f12', '#f5ab31'] },
];

export const chatStoryMusicOptions: ChatStoryMusicOption[] = [
    { id: 'none', label: 'No Background Music', src: '' },
    { id: 'midnight_pulse', label: 'Midnight Pulse', src: '/chatstory/music/midnight-pulse.mp3' },
    { id: 'neon_afterglow', label: 'Neon Afterglow', src: '/chatstory/music/neon-afterglow.mp3' },
    { id: 'late_night_bounce', label: 'Late Night Bounce', src: '/chatstory/music/late-night-bounce.mp3' },
];

export const chatStorySfxOptions: ChatStorySfxOption[] = [
    { id: 'message_send', label: 'Message Send', src: '/chatstory/sfx/message-send.wav' },
    { id: 'message_receive', label: 'iPhone Notification', src: '/chatstory/sfx/message-receive.wav' },
    { id: 'awkward_pause', label: 'Awkward Pause', src: '/chatstory/sfx/awkward-pause.wav' },
    { id: 'comical_disappointment', label: 'Comical Disappointment', src: '/chatstory/sfx/comical-disappointment.wav' },
];

export const makeChatStoryMessage = (role: ChatStoryRole, text: string, side: ChatStorySide): ChatStoryMessage => ({
    id: `${role}_${Math.random().toString(36).slice(2, 8)}`,
    role,
    side,
    text,
});

export const chatStorySampleMessages = (): ChatStoryMessage[] => [
    makeChatStoryMessage('receiver', 'would you still love me if i was a tank?', 'left'),
    makeChatStoryMessage('sender', 'well that depends on what tank you are...', 'right'),
    makeChatStoryMessage('sender', 'if you are a leopard 2 A7 then yes', 'right'),
    makeChatStoryMessage('receiver', 'that was not the romantic answer i expected', 'left'),
];

export const getChatStoryTheme = (themeId: string): ChatStoryTheme => (
    chatStoryThemes.find((item) => item.id === themeId) || chatStoryThemes[0]
);

export const getChatStoryBackground = (backgroundId: string): ChatStoryBackground => (
    chatStoryBackgrounds.find((item) => item.id === backgroundId) || chatStoryBackgrounds[0]
);

export const getChatStoryMusic = (musicId: string): ChatStoryMusicOption => (
    chatStoryMusicOptions.find((item) => item.id === musicId) || chatStoryMusicOptions[0]
);

export const getChatStoryVoiceLabel = (voices: StudioVoicePreset[], voiceId: string): string => (
    voices.find((voice) => voice.id === voiceId)?.name || voices[0]?.name || 'Core Neutral'
);
