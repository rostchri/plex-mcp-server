import json
import requests
import aiohttp
import asyncio
from plexapi.exceptions import NotFound # type: ignore
from modules import mcp, connect_to_plex
from urllib.parse import urljoin
import time
from typing import Optional, Union, List, Dict

def get_plex_headers(plex):
    """Get standard Plex headers for HTTP requests"""
    return {
        'X-Plex-Token': plex._token,
        'Accept': 'application/json'
    }

async def async_get_json(session, url, headers):
    """Helper function to make async HTTP requests"""
    async with session.get(url, headers=headers) as response:
        if response.status != 200:
            status = response.status
            try:
                error_body = await response.text()
                error_msg = error_body[:200]
            except:
                error_msg = "Could not read error body"
            raise Exception(f"Plex API error {status}: {error_msg}")
        return await response.json()

@mcp.tool()
async def library_list() -> str:
    """List all available libraries on the Plex server."""
    try:
        plex = connect_to_plex()
        libraries = plex.library.sections()
        
        if not libraries:
            return json.dumps({"message": "No libraries found on your Plex server."})
        
        libraries_dict = {}
        for lib in libraries:
            libraries_dict[lib.title] = {
                "type": lib.type,
                "libraryId": lib.key,
                "totalSize": lib.totalSize,
                "uuid": lib.uuid,
                "locations": lib.locations,
                "updatedAt": lib.updatedAt.isoformat()
            }
        
        return json.dumps(libraries_dict)
    except Exception as e:
        return json.dumps({"error": f"Error listing libraries: {str(e)}"})

@mcp.tool()
async def library_get_stats(library_name: str) -> str:
    """Get statistics for a specific library.

    Args:
        library_name: Name of the library to get stats for
    """
    BATCH_SIZE = 500

    try:
        plex = connect_to_plex()
        base_url = plex._baseurl
        headers = get_plex_headers(plex)

        async with aiohttp.ClientSession() as session:
            # First get library sections
            sections_url = urljoin(base_url, 'library/sections')
            sections_data = await async_get_json(session, sections_url, headers)

            target_section = None
            for section in sections_data['MediaContainer']['Directory']:
                if section['title'].lower() == library_name.lower():
                    target_section = section
                    break

            if not target_section:
                return json.dumps({"error": f"Library '{library_name}' not found"})

            section_id = target_section['key']
            library_type = target_section['type']

            # Create base result
            result = {
                "name": target_section['title'],
                "type": library_type,
                "totalItems": target_section.get('totalSize', 0)
            }

            # Helper: get count from endpoint without loading metadata.
            # Uses both query params (start/size) AND headers (X-Plex-Container-*)
            # to ensure Plex respects pagination. Requests only 1 item.
            async def get_count(url):
                sep = '&' if '?' in url else '?'
                count_url = f"{url}{sep}start=0&size=1"
                h = headers.copy()
                h['X-Plex-Container-Start'] = '0'
                h['X-Plex-Container-Size'] = '1'
                data = await async_get_json(session, count_url, h)
                mc = data['MediaContainer']
                return mc.get('totalSize', mc.get('size', 0))

            # Helper: async generator that yields items in paginated batches.
            # Mirrors the proven approach from library_get_contents: both
            # query params (start/size) AND headers (X-Plex-Container-*).
            async def iter_pages(url, batch_size=BATCH_SIZE):
                offset = 0
                total = None
                while True:
                    sep = '&' if '?' in url else '?'
                    page_url = f"{url}{sep}start={offset}&size={batch_size}"
                    h = headers.copy()
                    h['X-Plex-Container-Start'] = str(offset)
                    h['X-Plex-Container-Size'] = str(batch_size)
                    data = await async_get_json(session, page_url, h)
                    container = data['MediaContainer']
                    if total is None:
                        total = container.get('totalSize', container.get('size', 0))
                    items = container.get('Metadata', [])
                    if not items:
                        break
                    for item in items:
                        yield item
                    offset += len(items)
                    if offset >= total:
                        break

            all_url = urljoin(base_url, f'library/sections/{section_id}/all')
            unwatched_url = f"{all_url}?unwatched=1"

            if library_type == 'movie':
                # Get counts with size=0 — no metadata loaded into memory
                total_count, unwatched_count = await asyncio.gather(
                    get_count(all_url),
                    get_count(unwatched_url)
                )

                movie_stats = {
                    "count": total_count,
                    "unwatched": unwatched_count
                }

                # Aggregate stats by paginating through items in batches
                genres = {}
                directors = {}
                studios = {}
                decades = {}

                async for movie in iter_pages(all_url):
                    for genre in movie.get('Genre', []):
                        g = genre['tag']
                        genres[g] = genres.get(g, 0) + 1
                    for director in movie.get('Director', []):
                        d = director['tag']
                        directors[d] = directors.get(d, 0) + 1
                    studio = movie.get('studio')
                    if studio:
                        studios[studio] = studios.get(studio, 0) + 1
                    year = movie.get('year')
                    if year:
                        decade = (year // 10) * 10
                        decades[decade] = decades.get(decade, 0) + 1

                if genres:
                    movie_stats["topGenres"] = dict(sorted(genres.items(), key=lambda x: x[1], reverse=True)[:5])
                if directors:
                    movie_stats["topDirectors"] = dict(sorted(directors.items(), key=lambda x: x[1], reverse=True)[:5])
                if studios:
                    movie_stats["topStudios"] = dict(sorted(studios.items(), key=lambda x: x[1], reverse=True)[:5])
                if decades:
                    movie_stats["byDecade"] = dict(sorted(decades.items()))

                result["movieStats"] = movie_stats

            elif library_type == 'show':
                seasons_url = f"{all_url}?type=3"
                episodes_url = f"{all_url}?type=4"

                # Get all counts with size=0 — no metadata loaded
                total_count, unwatched_count, season_count, episode_count = await asyncio.gather(
                    get_count(all_url),
                    get_count(unwatched_url),
                    get_count(seasons_url),
                    get_count(episodes_url)
                )

                # Aggregate show-level stats by paginating through shows
                genres = {}
                studios = {}
                decades = {}

                async for show in iter_pages(all_url):
                    for genre in show.get('Genre', []):
                        g = genre['tag']
                        genres[g] = genres.get(g, 0) + 1
                    studio = show.get('studio')
                    if studio:
                        studios[studio] = studios.get(studio, 0) + 1
                    year = show.get('year')
                    if year:
                        decade = (year // 10) * 10
                        decades[decade] = decades.get(decade, 0) + 1

                show_stats = {
                    "shows": total_count,
                    "seasons": season_count,
                    "episodes": episode_count,
                    "unwatchedShows": unwatched_count
                }

                if genres:
                    show_stats["topGenres"] = dict(sorted(genres.items(), key=lambda x: x[1], reverse=True)[:5])
                if studios:
                    show_stats["topStudios"] = dict(sorted(studios.items(), key=lambda x: x[1], reverse=True)[:5])
                if decades:
                    show_stats["byDecade"] = dict(sorted(decades.items()))

                result["showStats"] = show_stats

            elif library_type == 'artist':
                tracks_url = f"{all_url}?type=10"

                # Get counts with size=0
                artist_count, track_count = await asyncio.gather(
                    get_count(all_url),
                    get_count(tracks_url)
                )

                artist_stats = {
                    "count": artist_count,
                    "totalTracks": track_count,
                    "totalAlbums": 0,
                    "totalPlays": 0
                }

                all_genres = {}
                all_years = {}
                top_albums = {}
                audio_formats = {}
                artist_albums = {}
                artist_plays = {}

                # Paginate through tracks in batches
                async for track in iter_pages(tracks_url):
                    artist_name = track.get('grandparentTitle', 'Unknown')
                    album_title = track.get('parentTitle', '')
                    track_views = track.get('viewCount', 0)

                    artist_stats["totalPlays"] += track_views

                    if artist_name not in artist_albums:
                        artist_albums[artist_name] = set()
                        artist_plays[artist_name] = 0
                    if album_title:
                        artist_albums[artist_name].add(album_title)
                    artist_plays[artist_name] += track_views

                    if album_title:
                        album_key = f"{artist_name} - {album_title}"
                        top_albums[album_key] = top_albums.get(album_key, 0) + track_views

                    for genre in track.get('Genre', []):
                        g = genre['tag']
                        all_genres[g] = all_genres.get(g, 0) + 1

                    year = track.get('parentYear') or track.get('year')
                    if year:
                        all_years[year] = all_years.get(year, 0) + 1

                    if track.get('Media') and track['Media']:
                        codec = track['Media'][0].get('audioCodec')
                        if codec:
                            audio_formats[codec] = audio_formats.get(codec, 0) + 1

                for artist_name, albums in artist_albums.items():
                    artist_stats["totalAlbums"] += len(albums)

                top_artists = {name: artist_plays.get(name, 0) for name in artist_albums}

                if all_genres:
                    artist_stats["topGenres"] = dict(sorted(all_genres.items(), key=lambda x: x[1], reverse=True)[:10])
                if top_artists:
                    artist_stats["topArtists"] = dict(sorted(top_artists.items(), key=lambda x: x[1], reverse=True)[:10])
                if top_albums:
                    artist_stats["topAlbums"] = dict(sorted(top_albums.items(), key=lambda x: x[1], reverse=True)[:10])
                if all_years:
                    artist_stats["byYear"] = dict(sorted(all_years.items()))
                if audio_formats:
                    artist_stats["audioFormats"] = audio_formats

                result["musicStats"] = artist_stats

            return json.dumps(result)

    except Exception as e:
        return json.dumps({"error": f"Error getting library stats: {str(e)}"})

@mcp.tool()
async def library_refresh(library_name: Optional[str] = None) -> str:
    """Refresh a specific library or all libraries.
    
    Args:
        library_name: Optional name of the library to refresh (refreshes all if None)
    """
    try:
        plex = connect_to_plex()
        
        if library_name:
            # Refresh a specific library
            section = None
            all_sections = plex.library.sections()
            
            # Find the section with matching name (case-insensitive)
            for s in all_sections:
                if s.title.lower() == library_name.lower():
                    section = s
                    break
            
            if not section:
                return json.dumps({"error": f"Library '{library_name}' not found. Available libraries: {', '.join([s.title for s in all_sections])}"})
            
            # Refresh the library
            section.refresh()
            return json.dumps({"success": True, "message": f"Refreshing library '{section.title}'. This may take some time."})
        else:
            # Refresh all libraries
            plex.library.refresh()
            return json.dumps({"success": True, "message": "Refreshing all libraries. This may take some time."})
    except Exception as e:
        return json.dumps({"error": f"Error refreshing library: {str(e)}"})

@mcp.tool()
async def library_scan(library_name: str, path: Optional[str] = None) -> str:
    """Scan a specific library or part of a library.
    
    Args:
        library_name: Name of the library to scan
        path: Optional specific path to scan within the library
    """
    try:
        plex = connect_to_plex()
        
        # Find the specified library
        section = None
        all_sections = plex.library.sections()
        
        # Find the section with matching name (case-insensitive)
        for s in all_sections:
            if s.title.lower() == library_name.lower():
                section = s
                break
        
        if not section:
            return json.dumps({"error": f"Library '{library_name}' not found. Available libraries: {', '.join([s.title for s in all_sections])}"})
        
        # Scan the library
        if path:
            try:
                section.update(path=path)
                return json.dumps({"success": True, "message": f"Scanning path '{path}' in library '{section.title}'. This may take some time."})
            except NotFound:
                return json.dumps({"error": f"Path '{path}' not found in library '{section.title}'."})
        else:
            section.update()
            return json.dumps({"success": True, "message": f"Scanning library '{section.title}'. This may take some time."})
    except Exception as e:
        return json.dumps({"error": f"Error scanning library: {str(e)}"})

@mcp.tool()
async def library_get_details(library_name: str) -> str:
    """Get detailed information about a specific library, including folder paths and settings.
    
    Args:
        library_name: Name of the library to get details for
    """
    try:
        plex = connect_to_plex()
        
        # Get all library sections
        all_sections = plex.library.sections()
        target_section = None
        
        # Find the section with the matching name (case-insensitive)
        for section in all_sections:
            if section.title.lower() == library_name.lower():
                target_section = section
                break
        
        if not target_section:
            return json.dumps({"error": f"Library '{library_name}' not found. Available libraries: {', '.join([s.title for s in all_sections])}"})
        
        # Create the result dictionary
        result = {
            "name": target_section.title,
            "type": target_section.type,
            "uuid": target_section.uuid,
            "totalItems": target_section.totalSize,
            "locations": target_section.locations,
            "agent": target_section.agent,
            "scanner": target_section.scanner,
            "language": target_section.language
        }
        
        # Get additional attributes using _data
        data = target_section._data
        
        # Add scanner settings if available
        if 'scannerSettings' in data:
            scanner_settings = {}
            for setting in data['scannerSettings']:
                if 'value' in setting:
                    scanner_settings[setting.get('key', 'unknown')] = setting['value']
            if scanner_settings:
                result["scannerSettings"] = scanner_settings
        
        # Add agent settings if available
        if 'agentSettings' in data:
            agent_settings = {}
            for setting in data['agentSettings']:
                if 'value' in setting:
                    agent_settings[setting.get('key', 'unknown')] = setting['value']
            if agent_settings:
                result["agentSettings"] = agent_settings
        
        # Add advanced settings if available
        if 'advancedSettings' in data:
            advanced_settings = {}
            for setting in data['advancedSettings']:
                if 'value' in setting:
                    advanced_settings[setting.get('key', 'unknown')] = setting['value']
            if advanced_settings:
                result["advancedSettings"] = advanced_settings
                
        return json.dumps(result)
    except Exception as e:
        return json.dumps({"error": f"Error getting library details: {str(e)}"})

@mcp.tool()
async def library_get_recently_added(count: int = 50, library_name: Optional[str] = None) -> str:
    """Get recently added media across all libraries or in a specific library.
    
    Args:
        count: Number of items to return (default: 50)
        library_name: Optional library name to limit results to
    """
    try:
        plex = connect_to_plex()
        
        # Check if we need to filter by library
        if library_name:
            # Find the specified library
            section = None
            all_sections = plex.library.sections()
            
            # Find the section with matching name (case-insensitive)
            for s in all_sections:
                if s.title.lower() == library_name.lower():
                    section = s
                    break
            
            if not section:
                return json.dumps({"error": f"Library '{library_name}' not found. Available libraries: {', '.join([s.title for s in all_sections])}"})
            
            # Get recently added from this library
            recent = section.recentlyAdded(maxresults=count)
        else:
            # Get recently added across all libraries
            recent = plex.library.recentlyAdded()
            # Sort by date added (newest first) and limit to count
            if recent:
                try:
                    recent = sorted(recent, key=lambda x: getattr(x, 'addedAt', None), reverse=True)[:count]
                except Exception as sort_error:
                    # If sorting fails, just take the first 'count' items
                    recent = recent[:count]
        
        if not recent:
            return json.dumps({"message": "No recently added items found."})
        
        # Prepare the result
        result = {
            "count": len(recent),
            "requestedCount": count,
            "library": library_name if library_name else "All Libraries",
            "items": {}
        }
        
        # Group results by type
        for item in recent:
            item_type = getattr(item, 'type', 'unknown')
            if item_type not in result["items"]:
                result["items"][item_type] = []
            
            try:
                added_at = str(getattr(item, 'addedAt', 'Unknown date'))
                
                if item_type == 'movie':
                    result["items"][item_type].append({
                        "title": item.title,
                        "year": getattr(item, 'year', ''),
                        "addedAt": added_at
                    })
                
                elif item_type == 'show':
                    result["items"][item_type].append({
                        "title": item.title,
                        "year": getattr(item, 'year', ''),
                        "addedAt": added_at
                    })
                
                elif item_type == 'season':
                    result["items"][item_type].append({
                        "showTitle": getattr(item, 'parentTitle', 'Unknown Show'),
                        "seasonNumber": getattr(item, 'index', '?'),
                        "addedAt": added_at
                    })
                
                elif item_type == 'episode':
                    result["items"][item_type].append({
                        "showTitle": getattr(item, 'grandparentTitle', 'Unknown Show'),
                        "seasonNumber": getattr(item, 'parentIndex', '?'),
                        "episodeNumber": getattr(item, 'index', '?'),
                        "title": item.title,
                        "addedAt": added_at
                    })
                
                elif item_type == 'artist':
                    result["items"][item_type].append({
                        "title": item.title,
                        "addedAt": added_at
                    })
                
                elif item_type == 'album':
                    result["items"][item_type].append({
                        "artist": getattr(item, 'parentTitle', 'Unknown Artist'),
                        "title": item.title,
                        "addedAt": added_at
                    })
                
                elif item_type == 'track':
                    result["items"][item_type].append({
                        "artist": getattr(item, 'grandparentTitle', 'Unknown Artist'),
                        "album": getattr(item, 'parentTitle', 'Unknown Album'),
                        "title": item.title,
                        "addedAt": added_at
                    })
                
                else:
                    # Generic handler for other types
                    result["items"][item_type].append({
                        "title": getattr(item, 'title', 'Unknown'),
                        "addedAt": added_at
                    })
            
            except Exception as format_error:
                # If there's an error formatting a particular item, just output basic info
                result["items"][item_type].append({
                    "title": getattr(item, 'title', 'Unknown'),
                    "error": str(format_error)
                })
        
        return json.dumps(result)
    except Exception as e:
        return json.dumps({"error": f"Error getting recently added items: {str(e)}"})

@mcp.tool()
async def library_get_contents(
    library_name: str, 
    unwatched: bool = False, 
    watched: bool = False,
    sort: Optional[str] = None, 
    offset: int = 0, 
    limit: int = 50,
    genre: Optional[str] = None,
    year: Optional[Union[int, str]] = None,
    content_rating: Optional[str] = None,
    director: Optional[str] = None,
    actor: Optional[str] = None,
    writer: Optional[str] = None,
    resolution: Optional[str] = None,
    network: Optional[str] = None,
    studio: Optional[str] = None
) -> str:
    """Get the filtered and paginated contents of a specific library.
    
    Args:
        library_name: Name of the library to get contents from
        unwatched: If True, only return unwatched items
        watched: If True, only return watched items
        sort: Sort order (e.g., 'addedAt:desc', 'title:asc')
        offset: Number of items to skip (default: 0)
        limit: Maximum number of items to return (default: 50)
        genre: Filter by genre tag
        year: Filter by release year
        content_rating: Filter by content rating (e.g., 'PG-13')
        director: Filter by director tag
        actor: Filter by actor tag
        writer: Filter by writer tag
        resolution: Filter by resolution (e.g., '4k', '1080')
        network: Filter by network tag (primarily for TV)
        studio: Filter by studio tag
    
    Returns:
        JSON string listing items in the library with pagination metadata
    """
    try:
        plex = connect_to_plex()
        base_url = plex._baseurl
        headers = get_plex_headers(plex)
        
        async with aiohttp.ClientSession() as session:
            # First get library sections
            sections_url = urljoin(base_url, 'library/sections')
            sections_data = await async_get_json(session, sections_url, headers)
            
            target_section = None
            for section in sections_data['MediaContainer']['Directory']:
                if section['title'].lower() == library_name.lower():
                    target_section = section
                    break
                    
            if not target_section:
                return json.dumps({"error": f"Library '{library_name}' not found"})
            
            section_id = target_section['key']
            library_type = target_section['type']
            
            from urllib.parse import urlencode
            
            # Build query parameters for filtering and pagination
            # Plex supports 'start' and 'size' as query parameters for library sections
            query_params = {
                'start': offset,
                'size': limit
            }
            if unwatched:
                query_params['unwatched'] = '1'
            elif watched:
                query_params['unwatched'] = '0'
            if sort:
                query_params['sort'] = sort
            
            # Add advanced filters
            if genre:
                query_params['genre'] = genre
            if year:
                query_params['year'] = str(year)
            if content_rating:
                query_params['contentRating'] = content_rating
            if director:
                query_params['director'] = director
            if actor:
                query_params['actor'] = actor
            if writer:
                query_params['writer'] = writer
            if resolution:
                query_params['resolution'] = resolution
            if network:
                query_params['network'] = network
            if studio:
                query_params['studio'] = studio
            
            # Also set pagination headers which Plex often expects/supports
            request_headers = headers.copy()
            request_headers['X-Plex-Container-Start'] = str(offset)
            request_headers['X-Plex-Container-Size'] = str(limit)
            
            # Get items with filters and pagination
            all_items_url = urljoin(base_url, f'library/sections/{section_id}/all?{urlencode(query_params)}')
            all_data = await async_get_json(session, all_items_url, request_headers)
            all_data = all_data['MediaContainer']
            
            # Prepare the result
            result = {
                "name": target_section['title'],
                "type": library_type,
                "totalItems": all_data.get('totalSize', all_data.get('size', 0)),
                "offset": offset,
                "limit": limit,
                "size": all_data.get('size', 0),
                "items": []
            }
            
            # Process items based on library type
            if library_type == 'movie':
                for item in all_data.get('Metadata', []):
                    year = item.get('year', 'Unknown')
                    duration = item.get('duration', 0)
                    # Convert duration from milliseconds to hours and minutes
                    hours, remainder = divmod(duration // 1000, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    
                    # Get media info
                    media_info = {}
                    if 'Media' in item:
                        media = item['Media'][0] if item['Media'] else {}
                        resolution = media.get('videoResolution', '')
                        codec = media.get('videoCodec', '')
                        if resolution and codec:
                            media_info = {
                                "resolution": resolution,
                                "codec": codec
                            }
                    
                    # Check if watched
                    watched = item.get('viewCount', 0) > 0
                    
                    result["items"].append({
                        "title": item.get('title', ''),
                        "year": year,
                        "duration": {
                            "hours": hours,
                            "minutes": minutes
                        },
                        "mediaInfo": media_info,
                        "watched": watched
                    })
            
            elif library_type == 'show':
                # Performance fix: childCount, leafCount and viewedLeafCount are already
                # present in the /library/sections/{id}/all response — no per-show
                # metadata requests needed.
                for item in all_data.get('Metadata', []):
                    year = item.get('year', 'Unknown')
                    season_count = item.get('childCount', 0)
                    episode_count = item.get('leafCount', 0)
                    watched_episodes = item.get('viewedLeafCount', 0)
                    watched = episode_count > 0 and watched_episodes == episode_count

                    result["items"].append({
                        "title": item.get('title', ''),
                        "year": year,
                        "seasonCount": season_count,
                        "episodeCount": episode_count,
                        "watched": watched
                    })
            
            elif library_type == 'artist':
                # Performance fix: fetch all artists' tracks in parallel using
                # asyncio.gather instead of sequential per-artist requests.
                artists = all_data.get('Metadata', [])

                async def fetch_artist_tracks(artist):
                    artist_id = artist.get('ratingKey')
                    if not artist_id:
                        return artist, {}
                    url = urljoin(base_url, f'library/sections/{section_id}/all?artist.id={artist_id}&type=10')
                    try:
                        data = await async_get_json(session, url, headers)
                        return artist, data
                    except Exception:
                        return artist, {}

                artist_results = await asyncio.gather(*[fetch_artist_tracks(a) for a in artists])

                for artist, tracks_data in artist_results:
                    artist_name = artist.get('title', '')
                    orig_view_count = artist.get('viewCount', 0)
                    orig_skip_count = artist.get('skipCount', 0)

                    albums = set()
                    track_count = 0
                    view_count = 0
                    skip_count = 0

                    mc = tracks_data.get('MediaContainer', {})
                    for track in mc.get('Metadata', []):
                        track_count += 1
                        if track.get('parentTitle'):
                            albums.add(track['parentTitle'])
                        view_count += track.get('viewCount', 0)
                        skip_count += track.get('skipCount', 0)

                    result["items"].append({
                        "title": artist_name,
                        "albumCount": len(albums),
                        "trackCount": track_count,
                        "viewCount": view_count if view_count > 0 else orig_view_count,
                        "skipCount": skip_count if skip_count > 0 else orig_skip_count
                    })
            
            else:
                # Generic handler for other types
                for item in all_data.get('Metadata', []):
                    result["items"].append({
                        "title": item.get('title', '')
                    })
            
            return json.dumps(result)
            
    except Exception as e:
        return json.dumps({"error": f"Error getting library contents: {str(e)}"})
