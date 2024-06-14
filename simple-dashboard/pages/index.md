---
title: Welcome to Evidence
---

<Details title='How to edit this page'>
  This page can be found in your project at `/pages/index.md`. Make a change to the markdown file and save it to see the change take effect in your browser.
</Details>

```sql artists
select name, familiarity from datasongs.artists order by familiarity desc limit 15
```

<BarChart 
    data={artists} 
    x=name
    y=familiarity
    swapXY=true 
    yAxisTitle="Top 15 Most Popular Artists" 
/>

```sql song_duration_distribution
select duration
from datasongs.songs
```

<Histogram
    data={song_duration_distribution}
    x=duration
/>
