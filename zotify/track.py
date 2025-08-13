from pathlib import Path, PurePath
import math
import re
import time
import uuid
from typing import Any, Tuple, List

from librespot.metadata import TrackId
import ffmpy

from zotify.const import TRACKS, ALBUM, GENRES, NAME, ITEMS, DISC_NUMBER, TRACK_NUMBER, IS_PLAYABLE, ARTISTS, IMAGES, URL, \
    RELEASE_DATE, ID, TRACKS_URL, FOLLOWED_ARTISTS_URL, SAVED_TRACKS_URL, TRACK_STATS_URL, CODEC_MAP, EXT_MAP, DURATION_MS, \
    HREF, ARTISTS, WIDTH
from zotify.termoutput import Printer, PrintChannel
from zotify.utils import fix_filename, set_audio_tags, set_music_thumbnail, create_download_directory, \
    get_directory_song_ids, add_to_directory_song_ids, get_previously_downloaded, add_to_archive, fmt_seconds
from zotify.zotify import Zotify
import traceback
from zotify.loader import Loader


def get_saved_tracks() -> list:
    """ Returns user's saved tracks """
    songs = []
    offset = 0
    limit = 50

    while True:
        resp = Zotify.invoke_url_with_params(
            SAVED_TRACKS_URL, limit=limit, offset=offset)
        offset += limit
        songs.extend(resp[ITEMS])
        if len(resp[ITEMS]) < limit:
            break

    return songs


def get_followed_artists() -> list:
    """ Returns user's followed artists """
    artists = []
    resp = Zotify.invoke_url(FOLLOWED_ARTISTS_URL)[1]
    for artist in resp[ARTISTS][ITEMS]:
        artists.append(artist[ID])
    
    return artists


def get_song_info(song_id) -> Tuple[List[str], List[Any], str, str, Any, Any, Any, Any, Any, Any, int]:
    """ Retrieves metadata for downloaded songs """
    with Loader(PrintChannel.PROGRESS_INFO, "Fetching track information..."):
        (raw, info) = Zotify.invoke_url(f'{TRACKS_URL}?ids={song_id}&market=from_token')

    if not TRACKS in info:
        raise ValueError(f'Invalid response from TRACKS_URL:\n{raw}')

    try:
        artists = []
        for data in info[TRACKS][0][ARTISTS]:
            artists.append(data[NAME])

        album_name = info[TRACKS][0][ALBUM][NAME]
        name = info[TRACKS][0][NAME]
        release_year = info[TRACKS][0][ALBUM][RELEASE_DATE].split('-')[0]
        disc_number = info[TRACKS][0][DISC_NUMBER]
        track_number = info[TRACKS][0][TRACK_NUMBER]
        scraped_song_id = info[TRACKS][0][ID]
        is_playable = info[TRACKS][0][IS_PLAYABLE]
        duration_ms = info[TRACKS][0][DURATION_MS]

        image = info[TRACKS][0][ALBUM][IMAGES][0]
        for i in info[TRACKS][0][ALBUM][IMAGES]:
            if i[WIDTH] > image[WIDTH]:
                image = i
        image_url = image[URL]

        return artists, info[TRACKS][0][ARTISTS], album_name, name, image_url, release_year, disc_number, track_number, scraped_song_id, is_playable, duration_ms
    except Exception as e:
        raise ValueError(f'Failed to parse TRACKS_URL response: {str(e)}\n{raw}')


def get_song_genres(rawartists: List[str], track_name: str) -> List[str]:
    if Zotify.CONFIG.get_save_genres():
        try:
            genres = []
            for data in rawartists:
                # query artist genres via href, which will be the api url
                with Loader(PrintChannel.PROGRESS_INFO, "Fetching artist information..."):
                    (raw, artistInfo) = Zotify.invoke_url(f'{data[HREF]}')
                if Zotify.CONFIG.get_all_genres() and len(artistInfo[GENRES]) > 0:
                    for genre in artistInfo[GENRES]:
                        genres.append(genre)
                elif len(artistInfo[GENRES]) > 0:
                    genres.append(artistInfo[GENRES][0])

            if len(genres) == 0:
                Printer.print(PrintChannel.WARNINGS, '###    No Genres found for song ' + track_name)
                genres.append('')

            return genres
        except Exception as e:
            raise ValueError(f'Failed to parse GENRES response: {str(e)}\n{raw}')
    else:
        return ['']


def get_song_lyrics(song_id: str, file_save: str) -> None:
    raw, lyrics = Zotify.invoke_url(f'https://spclient.wg.spotify.com/color-lyrics/v2/track/{song_id}')

    if lyrics:
        try:
            formatted_lyrics = lyrics['lyrics']['lines']
        except KeyError:
            raise ValueError(f'Failed to fetch lyrics: {song_id}')
        if(lyrics['lyrics']['syncType'] == "UNSYNCED"):
            with open(file_save, 'w+', encoding='utf-8') as file:
                for line in formatted_lyrics:
                    file.writelines(line['words'] + '\n')
            return
        elif(lyrics['lyrics']['syncType'] == "LINE_SYNCED"):
            with open(file_save, 'w+', encoding='utf-8') as file:
                for line in formatted_lyrics:
                    timestamp = int(line['startTimeMs'])
                    ts_minutes = str(math.floor(timestamp / 60000)).zfill(2)
                    ts_seconds = str(math.floor((timestamp % 60000) / 1000)).zfill(2)
                    ts_millis = str(math.floor(timestamp % 1000))[:2].zfill(2)
                    file.writelines(f'[{ts_minutes}:{ts_seconds}.{ts_millis}]' + line['words'] + '\n')
            return
    raise ValueError(f'Failed to fetch lyrics: {song_id}')


def get_song_duration(song_id: str) -> float:
    """ Retrieves duration of song in second as is on spotify """

    (raw, resp) = Zotify.invoke_url(f'{TRACK_STATS_URL}{song_id}')

    # get duration in miliseconds
    ms_duration = resp['duration_ms']
    # convert to seconds
    duration = float(ms_duration)/1000

    return duration


def download_track(mode: str, track_id: str, extra_keys=None, disable_progressbar=False) -> None:
    """ Downloads raw song audio from Spotify """
    # Fetch song info and set up variables
    artists, raw_artists, album_name, name, image_url, release_year, disc_number, track_number, scraped_song_id, is_playable, duration_ms = get_song_info(track_id)
    song_name = fix_filename(f"{artists[0]} - {name}")
    root_path = Zotify.CONFIG.get_root_path()
    filedir = str(PurePath(root_path).joinpath(artists[0], album_name))
    create_download_directory(filedir)
    ext = EXT_MAP[Zotify.CONFIG.get_download_format()]
    filename = f"{filedir}/{song_name}.{ext}"
    filename_temp = f"{filedir}/{song_name}.{ext}.part"
    check_id = False
    check_all_time = False
    show_progress = not disable_progressbar

    # Check if file already exists and is valid
    if Path(filename).exists() and Path(filename).stat().st_size > 0:
        # File exists and is not empty, skip download
        Printer.print(PrintChannel.SKIPS, f"\n###   SKIPPING: {song_name} (FILE ALREADY EXISTS)   ###\n")
        return
    # Check if song id is in archive (previously downloaded)
    elif Zotify.CONFIG.get_skip_previously_downloaded() and get_previously_downloaded(scraped_song_id):
        Printer.print(PrintChannel.SKIPS, f"\n###   SKIPPING: {song_name} (SONG ALREADY DOWNLOADED ONCE)   ###\n")
        return
    # Otherwise, proceed to download
    else:
        if track_id != scraped_song_id:
            track_id = scraped_song_id
        track = TrackId.from_base62(track_id)
        stream = Zotify.get_content_stream(track, Zotify.DOWNLOAD_QUALITY)
        create_download_directory(filedir)
        total_size = stream.input_stream.size

        prepare_download_loader = Loader(PrintChannel.PROGRESS_INFO, "Preparing download...")
        prepare_download_loader.stop()

        time_start = time.time()
        downloaded = 0
        with open(filename_temp, 'wb') as file, Printer.progress(
            desc=song_name,
            total=total_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
            disable=not show_progress
        ) as p_bar:
            max_iters = 100000
            iters = 0
            while True:
                data = stream.input_stream.stream().read(Zotify.CONFIG.get_chunk_size())
                if not data:
                    break
                p_bar.update(file.write(data))
                downloaded += len(data)
                if Zotify.CONFIG.get_download_real_time():
                    delta_real = time.time() - time_start
                    delta_want = (downloaded / total_size) * (duration_ms/1000)
                    sleep_time = delta_want - delta_real
                    # Only print debug output if --debug is set
                    if hasattr(Zotify, 'ARGS') and getattr(Zotify.ARGS, 'debug', False):
                        Printer.print(PrintChannel.PROGRESS_INFO, f"[DEBUG] delta_want: {delta_want:.2f}, delta_real: {delta_real:.2f}, sleep_time: {sleep_time:.2f}")
                    # Cap sleep time to 10 seconds max, and skip if negative or zero
                    if sleep_time > 0:
                        capped_sleep = min(sleep_time, 10)
                        if capped_sleep < sleep_time and hasattr(Zotify, 'ARGS') and getattr(Zotify.ARGS, 'debug', False):
                            Printer.print(PrintChannel.WARNINGS, f"[DEBUG] Sleep time capped to 10s (was {sleep_time:.2f}s)")
                        time.sleep(capped_sleep)
                iters += 1
                if iters > max_iters:
                    Printer.print(PrintChannel.WARNINGS, "[DEBUG] Max download iterations reached, breaking loop to prevent hang.")
                    break

        time_downloaded = time.time()
        genres = get_song_genres(raw_artists, name)
        if(Zotify.CONFIG.get_download_lyrics()):
            try:
                get_song_lyrics(track_id, PurePath(str(filename)[:-3] + "lrc"))
            except ValueError:
                Printer.print(PrintChannel.SKIPS, f"###   Skipping lyrics for {song_name}: lyrics not available   ###")
        convert_audio_format(filename_temp)
        try:
            set_audio_tags(filename_temp, artists, genres, name, album_name, release_year, disc_number, track_number)
            set_music_thumbnail(filename_temp, image_url)
        except Exception:
            Printer.print(PrintChannel.ERRORS, "Unable to write metadata, ensure ffmpeg is installed and added to your PATH.")

        if filename_temp != filename:
            Path(filename_temp).rename(filename)

        time_finished = time.time()

        # Try to display relative path, fallback to absolute if not possible
        try:
            rel_path = Path(filename).relative_to(Zotify.CONFIG.get_root_path())
        except ValueError:
            rel_path = Path(filename)

    Printer.print(PrintChannel.DOWNLOADS, f'###   Downloaded "{song_name}" to "{rel_path}" in {fmt_seconds(time_downloaded - time_start)} (plus {fmt_seconds(time_finished - time_downloaded)} converting)   ###' + "\n")
    # Always show the absolute path to the output file for user clarity
    Printer.print(PrintChannel.DOWNLOADS, f'###   Output file absolute path: {Path(filename).absolute()}   ###')

    # add song id to archive file
    if Zotify.CONFIG.get_skip_previously_downloaded():
        add_to_archive(scraped_song_id, PurePath(filename).name, artists[0], name)
    # add song id to download directory's .song_ids file
    if not check_id:
        add_to_directory_song_ids(filedir, scraped_song_id, PurePath(filename).name, artists[0], name)

    if Zotify.CONFIG.get_bulk_wait_time():
        time.sleep(Zotify.CONFIG.get_bulk_wait_time())

    prepare_download_loader.stop()


def convert_audio_format(filename) -> None:
    """ Converts raw audio into playable file """

    # Determine the correct output extension
    download_format = Zotify.CONFIG.get_download_format().lower()
    output_ext = download_format
    # Remove .part if present for ffmpeg output
    if filename.endswith('.part'):
        output_base = str(PurePath(filename).with_suffix(''))  # removes .part
    else:
        output_base = filename
    output_file = str(PurePath(output_base).with_suffix(f'.{output_ext}'))

    temp_filename = f'{PurePath(filename).parent}/.tmp_{uuid.uuid4().hex}'
    Path(filename).replace(temp_filename)

    # Use '-c:a flac' for FLAC output, otherwise use CODEC_MAP or 'copy' for matching formats
    if download_format == 'flac':
        output_params = ['-c:a', 'flac']
        bitrate = None  # FLAC is lossless, bitrate not needed
    else:
        file_codec = CODEC_MAP.get(download_format, 'copy')
        if file_codec != 'copy':
            bitrate = Zotify.CONFIG.get_transcode_bitrate()
            bitrates = {
                'auto': '320k' if Zotify.check_premium() else '160k',
                'normal': '96k',
                'high': '160k',
                'very_high': '320k'
            }
            bitrate = bitrates[Zotify.CONFIG.get_download_quality()]
        else:
            bitrate = None
        output_params = ['-c:a', file_codec]
        if bitrate:
            output_params += ['-b:a', bitrate]

    try:
        ff_m = ffmpy.FFmpeg(
            global_options=['-y', '-hide_banner', '-loglevel error'],
            inputs={temp_filename: None},
            outputs={output_file: output_params}
        )
        with Loader(PrintChannel.PROGRESS_INFO, "Converting file..."):
            ff_m.run()

        if Path(output_file).exists():
            # Conversion succeeded, move output_file to final filename
            Path(output_file).replace(filename)
            if Path(temp_filename).exists():
                Path(temp_filename).unlink()
        else:
            # Conversion failed, restore .part file if possible
            Printer.print(PrintChannel.ERRORS, f"###   Conversion failed: output file {output_file} not created. Restoring partial download.   ###")
            if Path(temp_filename).exists():
                Path(temp_filename).replace(filename)
            else:
                Printer.print(PrintChannel.ERRORS, f"###   Temp file {temp_filename} also missing. No output for this track.   ###")

    except ffmpy.FFExecutableNotFoundError:
        Printer.print(PrintChannel.WARNINGS, f'###   SKIPPING {file_codec.upper()} CONVERSION - FFMPEG NOT FOUND   ###')
        # Restore .part file if possible
        if Path(temp_filename).exists():
            Path(temp_filename).replace(filename)
