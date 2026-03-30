import { ChevronLeft, Phone, Video } from 'lucide-react';
import { getChatStoryBackground, getChatStoryTheme, type ChatStoryBackground, type ChatStoryMessage, type ChatStoryTheme } from '../lib/chatStoryConfig';

type PreviewSize = 'panel' | 'card';

type Props = {
    previewMode: 'video' | 'message';
    theme?: ChatStoryTheme;
    background?: ChatStoryBackground;
    backgroundVideoUrl?: string | null;
    characterName: string;
    avatarDataUrl?: string;
    messages: ChatStoryMessage[];
    size?: PreviewSize;
    className?: string;
};

const SIZE_CONFIG: Record<PreviewSize, { frame: string; phoneWrap: string; phoneInset: string; screenHeight: string; text: string; bubble: string; topBar: string; }> = {
    panel: {
        frame: 'min-h-[680px]',
        phoneWrap: 'h-[600px] w-[300px]',
        phoneInset: 'inset-[12px]',
        screenHeight: 'h-[508px]',
        text: 'text-[13px]',
        bubble: 'rounded-[20px] px-3 py-2',
        topBar: 'rounded-[24px] px-3 py-3',
    },
    card: {
        frame: 'min-h-[280px]',
        phoneWrap: 'h-[310px] w-[166px]',
        phoneInset: 'inset-[8px]',
        screenHeight: 'h-[244px]',
        text: 'text-[10px]',
        bubble: 'rounded-[14px] px-2 py-1.5',
        topBar: 'rounded-[16px] px-2 py-2',
    },
};

export default function ChatStoryPhonePreview({
    previewMode,
    theme = getChatStoryTheme('dark'),
    background = getChatStoryBackground('subway'),
    backgroundVideoUrl,
    characterName,
    avatarDataUrl,
    messages,
    size = 'panel',
    className = '',
}: Props) {
    const ui = SIZE_CONFIG[size];
    const visibleMessages = size === 'card' ? messages.slice(0, 2) : messages.slice(0, 8);

    return (
        <div className={`flex items-center justify-center rounded-[32px] border border-white/[0.06] bg-[#b5d8d8] px-4 py-6 ${ui.frame} ${className}`}>
            <div className={`relative ${ui.phoneWrap}`}>
                {previewMode === 'video' && backgroundVideoUrl ? (
                    <video
                        src={backgroundVideoUrl}
                        className="absolute inset-0 h-full w-full overflow-hidden rounded-[36px] object-cover opacity-80"
                        autoPlay
                        muted
                        loop
                        playsInline
                    />
                ) : previewMode === 'video' ? (
                    <div className={`absolute inset-0 overflow-hidden rounded-[36px] ${background.klass} opacity-75`} />
                ) : null}
                <div className={`absolute ${ui.phoneInset} rounded-[32px] border ${theme.shell} p-3 shadow-[0_20px_60px_rgba(0,0,0,0.45)]`}>
                    <div className={`${ui.topBar} ${theme.top}`}>
                        <div className="flex items-center justify-between">
                            <ChevronLeft className={size === 'card' ? 'h-4 w-4' : 'h-5 w-5'} />
                            <div className="text-center">
                                <div className={`mx-auto flex items-center justify-center overflow-hidden rounded-full bg-slate-400/70 text-sm font-bold text-white ${size === 'card' ? 'h-8 w-8' : 'h-9 w-9'}`}>
                                    {avatarDataUrl ? (
                                        <img src={avatarDataUrl} alt="Avatar" className="h-full w-full object-cover" />
                                    ) : (
                                        characterName.slice(0, 1).toUpperCase() || 'O'
                                    )}
                                </div>
                                <p className={`mt-1 font-semibold ${size === 'card' ? 'text-xs' : 'text-sm'}`}>{characterName || 'Omatic'}</p>
                            </div>
                            <div className={`flex items-center gap-2 ${size === 'card' ? 'text-xs' : 'text-sm'}`}>
                                <Video className={size === 'card' ? 'h-3.5 w-3.5 text-violet-200' : 'h-4 w-4 text-violet-200'} />
                                <Phone className={size === 'card' ? 'h-3.5 w-3.5 text-pink-300' : 'h-4 w-4 text-pink-300'} />
                            </div>
                        </div>
                    </div>
                    <div className={`mt-3 overflow-hidden rounded-[24px] ${theme.screen} p-4 ${ui.screenHeight}`}>
                        <div className="space-y-3">
                            {visibleMessages.length === 0 ? (
                                <div className="rounded-2xl border border-dashed border-white/10 px-4 py-6 text-center text-sm text-gray-400">
                                    Add sender and receiver blocks to preview the conversation.
                                </div>
                            ) : (
                                visibleMessages.map((message) => (
                                    <div key={message.id} className={`flex ${message.side === 'right' ? 'justify-end' : 'justify-start'}`}>
                                        <div className={`max-w-[78%] shadow ${ui.text} ${ui.bubble} ${message.side === 'right' ? theme.outgoing : theme.incoming}`}>
                                            {message.text || '...'}
                                        </div>
                                    </div>
                                ))
                            )}
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
