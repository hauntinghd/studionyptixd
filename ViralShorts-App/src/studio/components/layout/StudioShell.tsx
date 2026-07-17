import type { ReactNode } from 'react';
import type { PageNav } from '../NavBar';
import StudioTopBar from './StudioTopBar';

export default function StudioShell({
    onNavigate,
    sidebar,
    children,
    fullWidth,
    flush,
}: {
    onNavigate: PageNav;
    sidebar?: ReactNode;
    children: ReactNode;
    /** When true, content spans full width (e.g. builder without sidebar). */
    fullWidth?: boolean;
    /** Agent chat: no outer scroll — inner panel owns scrolling. */
    flush?: boolean;
}) {
    return (
        <div className="flex h-[100dvh] min-h-0 flex-col overflow-hidden bg-[#09090b] text-gray-100">
            <StudioTopBar onNavigate={onNavigate} />
            <div className="flex min-h-0 flex-1 overflow-hidden">
                {sidebar}
                <main
                    className={`min-h-0 min-w-0 flex-1 overflow-x-hidden ${
                        flush ? 'overflow-hidden p-0' : 'overflow-y-auto py-5 px-4 sm:px-6 lg:px-8'
                    } ${fullWidth && !flush ? 'px-4 sm:px-6' : ''}`}
                >
                    {children}
                </main>
            </div>
        </div>
    );
}
