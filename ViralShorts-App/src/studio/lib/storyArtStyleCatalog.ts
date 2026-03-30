import styleRows from './storyArtStyles.json';

export type StoryArtStyle = {
    id: string;
    label: string;
    desc: string;
    category: string;
    prompt: string;
};

export const storyArtStyleOptions = styleRows as StoryArtStyle[];
export const storyArtStyleCategoryOptions = Array.from(
    new Set(storyArtStyleOptions.map((style) => style.category)),
);
