import React, { useState } from 'react';
import { Video, Wand2, History, Shuffle, UploadCloud, Settings, PlayCircle, BarChart3, Edit3 } from 'lucide-react';

export default function App() {
    const [activeTab, setActiveTab] = useState<'create' | 'analyze' | 'edit'>('create');

    return (
        <div className="min-h-screen flex flex-col items-center pt-10 px-4 bg-gray-950">
            <header className="mb-12 text-center max-w-2xl">
                <h1 className="text-5xl font-extrabold tracking-tight mb-4 flex items-center justify-center gap-3">
                    <Wand2 className="w-10 h-10 text-indigo-500" />
                    Viral Shorts Studio
                </h1>
                <p className="text-gray-400 text-lg">
                    The ultimate engine for generating, analyzing, and editing high-retention short-form content.
                </p>
            </header>

            {/* Navigation */}
            <nav className="flex flex-wrap gap-2 p-1 bg-gray-900 rounded-xl mb-10 w-full max-w-4xl border border-gray-800">
                <button
                    onClick={() => setActiveTab('create')}
                    className={`flex-1 min-w-[150px] flex items-center justify-center gap-2 py-3 rounded-lg font-medium transition-all ${activeTab === 'create' ? 'bg-indigo-600 shadow-lg text-white' : 'text-gray-400 hover:text-white hover:bg-gray-800'}`}
                >
                    <Video className="w-5 h-5" />
                    Create Short
                </button>
                <button
                    onClick={() => setActiveTab('analyze')}
                    className={`flex-1 min-w-[150px] flex items-center justify-center gap-2 py-3 rounded-lg font-medium transition-all ${activeTab === 'analyze' ? 'bg-indigo-600 shadow-lg text-white' : 'text-gray-400 hover:text-white hover:bg-gray-800'}`}
                >
                    <BarChart3 className="w-5 h-5" />
                    Analyze Success
                </button>
                <button
                    onClick={() => setActiveTab('edit')}
                    className={`flex-1 min-w-[150px] flex items-center justify-center gap-2 py-3 rounded-lg font-medium transition-all ${activeTab === 'edit' ? 'bg-indigo-600 shadow-lg text-white' : 'text-gray-400 hover:text-white hover:bg-gray-800'}`}
                >
                    <Edit3 className="w-5 h-5" />
                    Pro AI Editor
                </button>
            </nav>

            {/* Main Content Area */}
            <main className="w-full max-w-4xl">
                {activeTab === 'create' && <CreateTab />}
                {activeTab === 'analyze' && <AnalyzeTab />}
                {activeTab === 'edit' && <EditorTab />}
            </main>
        </div>
    );
}

function CreateTab() {
    const [selectedTemplate, setSelectedTemplate] = useState('skeleton');

    const templates = [
        { id: 'skeleton', title: 'The Skeleton', desc: 'Core storytelling structure for high retention.', icon: <Settings className="w-6 h-6 text-gray-300" /> },
        { id: 'history', title: 'Historical Deep Dive', desc: 'Engaging, fast-paced historical facts template.', icon: <History className="w-6 h-6 text-amber-500" /> },
        { id: 'random', title: 'Randomized Chaos', desc: 'Algorithmically unpredictable hooks and cuts.', icon: <Shuffle className="w-6 h-6 text-emerald-400" /> }
    ];

    return (
        <div className="space-y-6">
            <div className="bg-gray-900 p-8 rounded-2xl border border-gray-800 shadow-2xl">
                <h2 className="text-2xl font-bold mb-6">Choose Your Template</h2>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    {templates.map(t => (
                        <div
                            key={t.id}
                            onClick={() => setSelectedTemplate(t.id)}
                            className={`p-6 rounded-xl border-2 cursor-pointer transition-all ${selectedTemplate === t.id ? 'border-indigo-500 bg-indigo-500/10' : 'border-gray-800 bg-gray-950 hover:border-gray-700'}`}
                        >
                            <div className="mb-4">{t.icon}</div>
                            <h3 className="font-semibold text-lg">{t.title}</h3>
                            <p className="text-gray-400 text-sm mt-2">{t.desc}</p>
                        </div>
                    ))}
                </div>
            </div>

            <div className="bg-gray-900 p-8 rounded-2xl border border-gray-800 shadow-xl">
                <h2 className="text-2xl font-bold mb-6">Upload Assets & Prompt</h2>
                <div className="border-2 border-dashed border-gray-700 rounded-xl p-10 text-center hover:bg-gray-800/50 transition-colors cursor-pointer group">
                    <UploadCloud className="w-12 h-12 mx-auto text-gray-500 group-hover:text-indigo-400 transition-colors mb-4" />
                    <p className="text-gray-300 font-medium">Drag and drop raw footage here</p>
                    <p className="text-sm text-gray-500 mt-2">or click to browse files (MP4, MOV)</p>
                </div>

                <div className="mt-6">
                    <label className="block text-sm font-medium text-gray-400 mb-2">Topic or Prompt</label>
                    <input
                        type="text"
                        placeholder="e.g., 'The fall of the Roman Empire but Gen Z slang'"
                        className="w-full bg-gray-950 border border-gray-800 rounded-lg px-4 py-3 text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
                    />
                </div>

                <button className="w-full mt-6 bg-indigo-600 hover:bg-indigo-500 text-white font-bold py-4 rounded-xl flex items-center justify-center gap-2 transition-transform active:scale-[0.98]">
                    <PlayCircle className="w-5 h-5" />
                    Generate Viral Short
                </button>
            </div>
        </div>
    );
}

function AnalyzeTab() {
    return (
        <div className="bg-gray-900 p-8 rounded-2xl border border-gray-800 shadow-xl">
            <div className="flex items-start justify-between mb-8">
                <div>
                    <h2 className="text-2xl font-bold text-emerald-400">Success Analyzer</h2>
                    <p className="text-gray-400 mt-2">Upload a high-performing short. AI will reverse-engineer its blueprint.</p>
                </div>
                <div className="p-3 bg-emerald-500/20 rounded-full">
                    <BarChart3 className="w-8 h-8 text-emerald-400" />
                </div>
            </div>

            <div className="border-2 border-dashed border-gray-700 rounded-xl p-10 text-center mb-8 hover:bg-gray-800/50 transition-colors cursor-pointer group">
                <UploadCloud className="w-12 h-12 mx-auto text-gray-500 group-hover:text-emerald-400 transition-colors mb-4" />
                <p className="text-gray-300 font-medium">Upload Viral Short to Analyze (MP4)</p>
            </div>

            <div className="p-6 bg-gray-950 rounded-xl border border-gray-800">
                <h3 className="font-semibold text-lg mb-4 text-emerald-400">Clone & Improve</h3>
                <p className="text-gray-400 text-sm mb-6">
                    Once analyzed, the engine will extract the exact pacing, hook structure, and visual style. You can then instantly dispatch a new topic into this exact successful shell.
                </p>

                <div className="space-y-4">
                    <div>
                        <label className="block text-sm font-medium text-gray-400 mb-2">New Topic for Cloned Style</label>
                        <input
                            type="text"
                            placeholder="e.g., 'AI Replacing Programmers'"
                            className="w-full bg-gray-900 border border-gray-700 rounded-lg px-4 py-3 text-white focus:outline-none focus:ring-2 focus:ring-emerald-500"
                            disabled
                        />
                    </div>
                    <button className="w-full px-6 py-4 bg-gray-800 hover:bg-emerald-600 hover:text-white border border-gray-700 font-bold rounded-xl transition-all flex justify-center items-center gap-2" disabled>
                        <Wand2 className="w-4 h-4" />
                        Generate Reskinned Video
                    </button>
                </div>
            </div>
        </div>
    );
}

function EditorTab() {
    return (
        <div className="bg-gray-900 p-8 rounded-2xl border border-gray-800 shadow-xl">
            <div className="flex items-start justify-between mb-8">
                <div>
                    <h2 className="text-2xl font-bold text-pink-500">Pro-Gen AI Editor</h2>
                    <p className="text-gray-400 mt-2">Upload product videos for clients. AI edits to premium agency standards.</p>
                </div>
                <div className="p-3 bg-pink-500/20 rounded-full">
                    <Edit3 className="w-8 h-8 text-pink-400" />
                </div>
            </div>

            <div className="border-2 border-dashed border-gray-700 rounded-xl p-10 text-center mb-8 hover:bg-gray-800/50 transition-colors cursor-pointer group">
                <UploadCloud className="w-12 h-12 mx-auto text-gray-500 group-hover:text-pink-400 transition-colors mb-4" />
                <p className="text-gray-300 font-medium">Upload Client / Product Footage</p>
                <p className="text-sm text-gray-500 mt-2">High Quality Raw Files Recommended</p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="p-6 bg-gray-950 rounded-lg border border-gray-800">
                    <h4 className="font-semibold text-white mb-4">Automated VFX Stack:</h4>
                    <ul className="text-sm text-gray-400 space-y-3">
                        <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-pink-500"></div> Dynamic Auto-Framing</li>
                        <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-pink-500"></div> Cinema-grade Color Grading</li>
                        <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-pink-500"></div> Kinetic Typography Subtitles</li>
                        <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-pink-500"></div> SFX & Audio Sweetening</li>
                    </ul>
                </div>
                <div className="p-6 bg-gray-950 rounded-lg border border-gray-800 flex flex-col justify-center items-center text-center">
                    <span className="text-4xl mb-3 font-extrabold text-transparent bg-clip-text bg-gradient-to-r from-pink-500 to-indigo-500">Level 10</span>
                    <span className="text-xs text-gray-400 uppercase tracking-widest font-bold">World Class Output</span>
                    <p className="text-xs text-gray-600 mt-4 max-w-[150px]">Competes directly with top-tier human editors.</p>
                </div>
            </div>

            <button className="w-full mt-8 bg-gradient-to-r from-pink-600 to-indigo-600 hover:from-pink-500 hover:to-indigo-500 text-white font-bold py-4 rounded-xl shadow-lg transition-transform active:scale-[0.98] text-lg">
                Start Magical Edit
            </button>
        </div>
    );
}
