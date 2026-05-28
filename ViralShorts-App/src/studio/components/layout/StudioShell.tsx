import type { ReactNode } from 'react';
import type { PageNav } from '../NavBar';
import StudioTopBar from './StudioTopBar';

export default function StudioShell({
    onNavigate,
    sidebar,
    children,
    fullWidth,
}: {
    onNavigate: PageNav;
    sidebar?: ReactNode;
    children: ReactNode;
    /** When true, content spans full width (e.g. builder without sidebar). */
    fullWidth?: boolean;
}) {
    return (
        <div className="flex min-h-screen flex-col bg-[#09090b] text-gray-100">
            <StudioTopBar onNavigate={onNavigate} />
            <div className="flex min-h-0 flex-1">
                {sidebar}
                <main
                    className={`min-w-0 flex-1 overflow-x-hidden overflow-y-auto py-5 ${
                        fullWidth ? 'px-4 sm:px-6' : 'px-4 sm:px-6 lg:px-8'
                    }`}
                >
                    {children}
                </main>
            </div>
        </div>
    );
}
