import functools

from distinctipy import distinctipy
from PIL import ImageFont, Image, ImageDraw

from entity import TimeRange, TimeSeriesRange

WIDEN_PER_FRAGMENT = 150
HEIGHT_PER_FRAGMENT = 150

TEXT_WIDEN_DEVIATION = 5
TEXT_HEIGHT_DEVIATION = 5

BORDER = 50

RIGHT_EXTRA_SPACE = 50

HEIGHT_PER_STORAGE = 25


def plot(storage_engines, storage_units, fragments, target):
    series_bounds = set()
    time_bounds = set()
    for fragment in fragments:
        series_bounds.add(fragment.ts_range.start)
        series_bounds.add(fragment.ts_range.end)
        time_bounds.add(fragment.time_range.start)
        time_bounds.add(fragment.time_range.end)

    series_bounds_map = get_series_bounds_map(series_bounds)

    time_bounds_map = get_time_bounds_map(time_bounds)

    def get_fragment_storage_engine(fragment):
        unit_id = fragment.storage_unit_id
        storage_id = None
        for storage_unit in storage_units:
            if storage_unit.id != unit_id:
                continue
            storage_id = storage_unit.storage_id
            break

        if storage_id is None:
            raise ValueError("invalid storage unit id " + str(unit_id))
        for storage_engine in storage_engines:
            if storage_engine.id == storage_id:
                return storage_engine
        raise ValueError("invalid storage engine id " + str(storage_id))

    colors = distinctipy.get_colors(len(storage_engines))
    colors = [distinctipy.get_hex(color) for color in colors]

    def calculate_boundary():
        w, h = BORDER * 2 + WIDEN_PER_FRAGMENT * (len(time_bounds_map) - 1) + RIGHT_EXTRA_SPACE, BORDER * 3 + HEIGHT_PER_FRAGMENT * (len(series_bounds_map) - 1) + len(storage_engines) * HEIGHT_PER_STORAGE
        if w < 320:
            w = 320
        return w, h

    def get_coordinate_for_fragment(ts, series):
        return BORDER + time_bounds_map[ts] * WIDEN_PER_FRAGMENT, BORDER + series_bounds_map[series] * HEIGHT_PER_FRAGMENT

    def get_coordinate_for_text(ts, series):
        index_x, index_y = get_coordinate_for_fragment(ts, series)
        return index_x + TEXT_WIDEN_DEVIATION, index_y + TEXT_HEIGHT_DEVIATION

    def get_coordinate_for_storage_engine(index):
        return BORDER, BORDER * 2 + HEIGHT_PER_FRAGMENT * (len(series_bounds_map) - 1) + index * HEIGHT_PER_STORAGE

    def get_text_for_coordinate(ts, series):
        if series == TimeSeriesRange.UNBOUNDED_FROM or series == TimeSeriesRange.UNBOUNDED_TO:
            series = "null"
        if ts == 0:
            ts = "min"
        if ts == TimeRange.MAX_TIME:
            ts = "max"
        return str(series) + ", " + str(ts)

    image = Image.new('RGB', calculate_boundary(), 'white')

    text_font = ImageFont.truetype("alibaba.ttf", 10)
    storage_font = ImageFont.truetype("alibaba.ttf", 10)
    draw = ImageDraw.Draw(image)

    for fragment in fragments:
        start_ts, end_ts = fragment.time_range.start, fragment.time_range.end
        start_series, end_series = fragment.ts_range.start, fragment.ts_range.end
        from_x, from_y = get_coordinate_for_fragment(start_ts, start_series)
        to_x, to_y = get_coordinate_for_fragment(end_ts, end_series)
        draw.rectangle((from_x, from_y, to_x, to_y), fill=colors[get_fragment_storage_engine(fragment).id], outline='black')
        draw.text(get_coordinate_for_text(start_ts, start_series), font=text_font, text=get_text_for_coordinate(start_ts, start_series), fill='black')
        draw.text(get_coordinate_for_text(end_ts, end_series), font=text_font, text=get_text_for_coordinate(end_ts, end_series), fill='black', spacing=5)

    for i in range(len(storage_engines)):
        storage_engine = storage_engines[i]
        draw.text(get_coordinate_for_storage_engine(i), font=storage_font, text=str(storage_engine), fill=colors[storage_engine.id])

    image.save(target)


def compare_series(x, y):
    if x == y:
        return 0
    if x == TimeSeriesRange.UNBOUNDED_FROM or y == TimeSeriesRange.UNBOUNDED_TO:
        return -1
    if x == TimeSeriesRange.UNBOUNDED_TO or y == TimeSeriesRange.UNBOUNDED_FROM:
        return 1
    return  -1 if x < y else 1

def get_series_bounds_map(series_bounds):
    series_bounds = list(series_bounds)
    series_bounds.sort(key=functools.cmp_to_key(compare_series))
    series_bounds_map = {}
    for i in range(len(series_bounds)):
        series_bounds_map[series_bounds[i]] = i
    return series_bounds_map


def get_time_bounds_map(time_bounds):
    time_bounds = list(time_bounds)
    time_bounds.sort()
    time_bounds_map = {}
    for i in range(len(time_bounds)):
        time_bounds_map[time_bounds[i]] = i
    return time_bounds_map