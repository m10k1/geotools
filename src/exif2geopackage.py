from typing import List, Tuple, Dict, Optional, Any
import hydra
from hydra.core.config_store import ConfigStore
from dataclasses import dataclass

import os
from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS
import geopandas as gpd
from shapely.geometry import Point
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from tqdm import tqdm

@dataclass
class Config:
    """
    Hydraの設定クラス

    Attributes:
        root_folder (str): 画像ファイルが保存されているルートディレクトリのパス。
        output_file (str): 出力されるGeoPackageのファイル名。
    """

    root_folder: str = "path/to/your/images"
    output_file: str = "output.gpkg"

cs = ConfigStore.instance()
cs.store(name="config", node=Config)


def get_exif(filename: str) -> Dict[str, Any]:
    """
    指定されたJPEGファイルからEXIF情報を抽出する。

    Args:
        filename (str): EXIF情報を抽出するJPEGファイルのパス。

    Returns:
        Dict[str, Any]: 抽出されたEXIF情報を含む辞書。
    """

    try:
        image = Image.open(filename)
        exif_data = {}
        if hasattr(image, '_getexif'):
            exif_info = image._getexif()
            if exif_info is not None:
                for tag, value in exif_info.items():
                    decoded = TAGS.get(tag, tag)
                    if decoded == "GPSInfo":
                        gps_data = {}
                        for t in value:
                            sub_decoded = GPSTAGS.get(t, t)
                            gps_data[sub_decoded] = value[t]
                        exif_data[decoded] = gps_data
                    else:
                        exif_data[decoded] = value
        return filename, exif_data
    except Exception as e:
        print(f"Error reading EXIF data from {filename}: {e}")
        return filename, None

def get_geolocation(exif_data):
    """EXIF情報から緯度経度情報を取得する"""
    if "GPSInfo" in exif_data:
        gps_info = exif_data["GPSInfo"]
        gps_latitude = gps_info.get("GPSLatitude")
        gps_latitude_ref = gps_info.get("GPSLatitudeRef")
        gps_longitude = gps_info.get("GPSLongitude")
        gps_longitude_ref = gps_info.get("GPSLongitudeRef")
        
        if gps_latitude and gps_latitude_ref and gps_longitude and gps_longitude_ref:
            lat = convert_to_degrees(gps_latitude)
            if gps_latitude_ref != "N":                
                lat = 0 - lat

            lon = convert_to_degrees(gps_longitude)
            if gps_longitude_ref != "E":
                lon = 0 - lon

            return lat, lon
    return None, None


def convert_to_degrees(value):
    """GPSの緯度または経度を度に変換する"""
    d, m, s = value
    return d + (m / 60.0) + (s / 3600.0)


def process_image(filename: str) -> Optional[Dict[str, Any]]:
    """
    画像ファイルを処理し、地理的位置情報を含む辞書を返す。

    Args:
        filename (str): 処理する画像ファイルのパス。

    Returns:
        Optional[Dict[str, Any]]: 地理的位置情報（存在する場合）。そうでなければNone。
    """

    _, exif_data = get_exif(filename)
    if exif_data:
        lat, lon = get_geolocation(exif_data)
        if lat is not None and lon is not None:
            return {'geometry': Point(lon, lat), 'filename': filename}
    return None

def find_jpegs(root_folder: str) -> List[str]:
    """
    指定されたディレクトリとそのサブディレクトリからJPEGファイルのリストを生成する。

    Args:
        root_folder (str): JPEGファイルを検索するルートディレクトリのパス。

    Returns:
        List[str]: 発見されたJPEGファイルのパスのリスト。
    """

    jpegs = []
    for root, dirs, files in os.walk(root_folder):
        for file in files:
            if file.lower().endswith(".jpeg") or file.lower().endswith(".jpg"):
                jpegs.append(os.path.join(root, file))
    return jpegs

def create_geopackage(jpegs: List[str], output_file: str = "output.gpkg") -> None:
    """
    指定されたJPEGファイルリストからGeoPackageを作成する。

    Args:
        jpegs (List[str]): 処理するJPEGファイルのリスト。
        output_file (str): 生成されるGeoPackageのファイル名。
    """

    data: List[Dict[str, Any]] = []
    with ProcessPoolExecutor() as executor:
        futures: List[Future] = {executor.submit(process_image, jpeg): jpeg for jpeg in jpegs}

        for future in tqdm(as_completed(futures), total=len(futures)):
            result = future.result()
            if result:
                data.append(result)
    
    if data:
        gdf = gpd.GeoDataFrame(data, crs="EPSG:4326")
        gdf.to_file(output_file, driver="GPKG")


@hydra.main(config_name="config")
def main(cfg: Config) -> None:
    """
    メイン関数。設定に基づいてJPEGファイルを検索し、GeoPackageを生成する。

    Args:
        cfg (Config): Hydraによって提供される設定。
    """

    # フォルダからjpegを探してリスト化
    jpegs: List[str] = find_jpegs(cfg.root_folder)
    print(f"Found {len(jpegs)} JPEG files.")

    # 
    create_geopackage(jpegs, cfg.output_file)
    print(f"Geopackage has been created.")

# メイン処理の実行
if __name__ == "__main__":
    main()