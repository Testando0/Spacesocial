declare module "yt-search" {
  type YouTubeSearchResult = {
    videos?: Array<{
      type?: string;
      videoId?: string;
      url?: string;
      title?: string;
      description?: string;
      image?: string;
      thumbnail?: string;
      timestamp?: string;
      duration?: {
        seconds?: number;
        timestamp?: string;
      };
      author?: {
        name?: string;
        url?: string;
      };
    }>;
  };

  function ytSearch(query: string): Promise<YouTubeSearchResult>;
  export default ytSearch;
}