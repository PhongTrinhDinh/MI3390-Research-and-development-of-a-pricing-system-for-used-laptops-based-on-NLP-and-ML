import os
import re
import pytest
from src.data_pipeline.taxonomy_resolver import TaxonomyResolver, build_alias_pattern, normalize_str


@pytest.fixture(scope="module")
def resolver():
    """Fixture khởi tạo một instance TaxonomyResolver dùng chung cho toàn bộ test."""
    return TaxonomyResolver()


# ==============================================================================
# 1. Kiểm tra nạp từ điển & Cấu trúc dữ liệu
# ==============================================================================

def test_load_dictionary_success(resolver):
    """Kiểm tra từ điển được nạp đầy đủ các phân hệ chính."""
    assert resolver.data is not None
    expected_keys = ["brands", "series", "cpu", "ram", "storage", "resolution", "gpu", "os"]
    for key in expected_keys:
        assert key in resolver.data, f"Thiếu phân hệ {key} trong taxonomy dictionary"

    assert len(resolver.data["brands"]) >= 15
    assert len(resolver.data["series"]) >= 30
    assert len(resolver.alias_to_brand) > 0
    assert len(resolver.alias_to_series) > 0


def test_load_dictionary_file_not_found():
    """Kiểm tra báo lỗi chuẩn xác khi đường dẫn file từ điển không tồn tại."""
    with pytest.raises(FileNotFoundError):
        TaxonomyResolver(dict_path="non_existent_taxonomy_file.json")


# ==============================================================================
# 2. Kiểm tra hàm Helper: build_alias_pattern & normalize_str
# ==============================================================================

def test_normalize_str():
    """Kiểm tra hàm chuẩn hóa chuỗi (NFC, lower, xóa khoảng trắng thừa)."""
    assert normalize_str("  MacBook   Pro   ") == "macbook pro"
    assert normalize_str("ASUS   ROG  ") == "asus rog"
    assert normalize_str(None) == ""
    assert normalize_str("") == ""


def test_build_alias_pattern_order_and_escaping():
    """
    Kiểm tra thuật toán sinh pattern regex:
    1. Sắp xếp cụm từ dài trước cụm từ ngắn (macbook pro trước macbook).
    2. Thoát đúng các ký tự đặc biệt (+, ., v.v.).
    """
    aliases = ["macbook", "macbook pro", "fhd+", "2.8k"]
    pat_py = build_alias_pattern(aliases, dialect="python")
    
    # Thứ tự: macbook\s+pro phải xuất hiện trước macbook đơn lẻ
    inner = re.search(r"\(\?:(.*?)\)", pat_py).group(1)
    branches = inner.split("|")
    assert branches.index(r"macbook\s+pro") < branches.index("macbook"), "macbook pro phải đứng trước macbook trong regex"

    # Thoát ký tự đặc biệt + và .
    assert r"fhd\+" in branches
    assert r"2\.8k" in branches


# ==============================================================================
# 3. Kiểm tra phân giải Thiết bị (Series -> Brand -> Category)
# ==============================================================================

def test_resolve_series_and_inferred_brand_apple(resolver):
    """Kiểm tra nhận diện dòng MacBook: suy luận hãng Apple, category Professional/Ultrabook."""
    res_pro = resolver.resolve_device("Cần bán macbook pro m2 16gb sạc ít lần")
    assert res_pro["canonical_series"] == "MacBook Pro"
    assert res_pro["canonical_brand"] == "Apple"
    assert res_pro["device_category"] == "Professional"
    assert res_pro["default_os"] == "macOS"
    assert res_pro["default_cpu_brand"] == "Apple"
    assert res_pro["brand_source"] == "inferred_from_series"
    assert res_pro["series_source"] == "title"

    res_air = resolver.resolve_device("Laptop mb air m1 8gb gold chính hãng")
    assert res_air["canonical_series"] == "MacBook Air"
    assert res_air["canonical_brand"] == "Apple"
    assert res_air["device_category"] == "Ultrabook"


def test_resolve_series_and_inferred_brand_lenovo(resolver):
    """Kiểm tra nhận diện dòng Lenovo: ThinkPad, Legion, LOQ, IdeaPad Slim."""
    # ThinkPad
    res_tp = resolver.resolve_device("Thanh lý thinkpad x1 carbon gen 9 keng")
    assert res_tp["canonical_series"] == "ThinkPad"
    assert res_tp["canonical_brand"] == "Lenovo"
    assert res_tp["device_category"] == "Enterprise / Business"

    # Legion
    res_legion = resolver.resolve_device("Laptop gaming legion 5 pro rtx 3060")
    assert res_legion["canonical_series"] == "Legion"
    assert res_legion["canonical_brand"] == "Lenovo"
    assert res_legion["device_category"] == "Enthusiast Gaming"

    # LOQ
    res_loq = resolver.resolve_device("Lenovo LOQ 15 i5 13420H RTX 4050")
    assert res_loq["canonical_series"] == "LOQ"
    assert res_loq["canonical_brand"] == "Lenovo"
    assert res_loq["device_category"] == "Entry Gaming"


def test_resolve_series_asus_and_hp(resolver):
    """Kiểm tra các dòng Asus (TUF, Vivobook, ROG) và HP (Victus, Omnibook)."""
    # ASUS TUF
    res_tuf = resolver.resolve_device("Asus tuf gaming a15 r7 7735hs")
    assert res_tuf["canonical_series"] == "TUF Gaming"
    assert res_tuf["canonical_brand"] == "ASUS"
    assert res_tuf["device_category"] == "Gaming"

    # ASUS Vivobook
    res_vivo = resolver.resolve_device("Laptop asus vivobook 15 oled 2.8k")
    assert res_vivo["canonical_series"] == "Vivobook"
    assert res_vivo["canonical_brand"] == "ASUS"
    assert res_vivo["device_category"] == "Mainstream / Thin & Light"

    # HP Victus
    res_vic = resolver.resolve_device("HP victus 16 i7 13700h rtx 4060")
    assert res_vic["canonical_series"] == "Victus"
    assert res_vic["canonical_brand"] == "HP"
    assert res_vic["device_category"] == "Gaming"

    # HP Omnibook (AI PC mới)
    res_omni = resolver.resolve_device("Laptop HP Omnibook X Snapdragon X Elite")
    assert res_omni["canonical_series"] == "Omnibook"
    assert res_omni["canonical_brand"] == "HP"
    assert res_omni["device_category"] == "AI PC / Ultrabook"


def test_resolve_device_direct_brand_without_series(resolver):
    """Kiểm tra khi tiêu đề chỉ có tên hãng trực tiếp chứ không có tên dòng máy."""
    res = resolver.resolve_device("Laptop dell i5 ram 8gb ssd 256gb")
    assert res["canonical_brand"] == "Dell"
    assert res["canonical_series"] is None
    assert res["device_category"] is None
    assert res["brand_source"] == "direct_title"
    assert res["default_os"] == "Windows 11"


def test_resolve_device_description_fallback(resolver):
    """Kiểm tra ưu tiên: Tiêu đề không có series, nhưng mô tả có series."""
    res = resolver.resolve_device(
        title="Bán laptop cũ giá rẻ sinh viên",
        description="Máy là dòng dell latitude 7400 mỏng nhẹ bền bỉ"
    )
    assert res["canonical_series"] == "Latitude"
    assert res["canonical_brand"] == "Dell"
    assert res["device_category"] == "Enterprise Business"
    assert res["series_source"] == "description"
    assert res["brand_source"] == "inferred_from_series"


def test_resolve_device_pc_brand_code_fallback(resolver):
    """Kiểm tra fallback từ pc_brand == 3 của Chợ Tốt khi tiêu đề/mô tả không có chữ Apple."""
    res = resolver.resolve_device(
        title="Máy đẹp keng như mới pin 100",
        description="Máy dùng văn phòng mượt mà",
        pc_brand_code=3
    )
    assert res["canonical_brand"] == "Apple"
    assert res["brand_source"] == "pc_brand_code"
    assert res["default_os"] == "macOS"


# ==============================================================================
# 4. Kiểm tra phân giải CPU
# ==============================================================================

def test_resolve_cpu_intel(resolver):
    """Kiểm tra nhận diện CPU Intel (Core Ultra, Core i)."""
    # Core Ultra
    cpu_u7 = resolver.resolve_cpu("Asus Zenbook trang bị core ultra 7 155h")
    assert cpu_u7 is not None
    assert cpu_u7["canonical_tier"] == "Core Ultra 7"
    assert cpu_u7["cpu_brand"] == "Intel"

    # Core i5
    cpu_i5 = resolver.resolve_cpu("Dell Inspiron i5 1240P")
    assert cpu_i5 is not None
    assert cpu_i5["canonical_tier"] == "Core i5"
    assert cpu_i5["cpu_brand"] == "Intel"


def test_resolve_cpu_amd(resolver):
    """Kiểm tra nhận diện CPU AMD (Ryzen 7, Ryzen AI 9)."""
    cpu_r7 = resolver.resolve_cpu("Lenovo Legion 5 chạy ryzen 7 5800h")
    assert cpu_r7 is not None
    assert cpu_r7["canonical_tier"] == "Ryzen 7"
    assert cpu_r7["cpu_brand"] == "AMD"

    cpu_ai9 = resolver.resolve_cpu("Asus Zenbook S16 ryzen ai 9 hx 370")
    assert cpu_ai9 is not None
    assert cpu_ai9["canonical_tier"] == "Ryzen AI 9"
    assert cpu_ai9["cpu_brand"] == "AMD"


def test_resolve_cpu_apple_and_qualcomm(resolver):
    """Kiểm tra nhận diện CPU Apple Silicon và Qualcomm Snapdragon."""
    cpu_m3 = resolver.resolve_cpu("Macbook Pro m3 pro 18gb ram")
    assert cpu_m3 is not None
    assert cpu_m3["canonical_tier"] == "Apple M3 Pro"
    assert cpu_m3["cpu_brand"] == "Apple"

    cpu_snap = resolver.resolve_cpu("Dell XPS chạy snapdragon x elite")
    assert cpu_snap is not None
    assert cpu_snap["canonical_tier"] == "Snapdragon X Elite"
    assert cpu_snap["cpu_brand"] == "Qualcomm"


# ==============================================================================
# 5. Kiểm tra phân giải GPU
# ==============================================================================

def test_resolve_gpu_discrete(resolver):
    """Kiểm tra nhận diện Card đồ họa rời (RTX 4060, RTX 3050, GTX 1650)."""
    gpu_4060 = resolver.resolve_gpu("Laptop Asus Gaming vga 8gb rtx4060")
    assert gpu_4060 is not None
    assert gpu_4060["canonical_name"] == "NVIDIA GeForce RTX 4060"
    assert gpu_4060["gpu_type"] == "discrete"
    assert gpu_4060["vram_gb"] == 8

    gpu_3050 = resolver.resolve_gpu("HP Victus rtx 3050 4gb")
    assert gpu_3050 is not None
    assert gpu_3050["canonical_name"] == "NVIDIA GeForce RTX 3050"
    assert gpu_3050["gpu_type"] == "discrete"
    assert gpu_3050["vram_gb"] == 4


def test_resolve_gpu_integrated(resolver):
    """Kiểm tra nhận diện Card Onboard / Integrated (Intel Iris Xe, Radeon 780M, Arc)."""
    gpu_iris = resolver.resolve_gpu("Laptop mỏng nhẹ đồ họa iris xe")
    assert gpu_iris is not None
    assert gpu_iris["canonical_name"] == "Intel Iris Xe Graphics"
    assert gpu_iris["gpu_type"] == "integrated"
    assert gpu_iris["vram_gb"] is None

    gpu_arc = resolver.resolve_gpu("Chip Core Ultra tích hợp intel arc")
    assert gpu_arc is not None
    assert gpu_arc["canonical_name"] == "Intel Arc Graphics"
    assert gpu_arc["gpu_type"] == "integrated"


# ==============================================================================
# 6. Kiểm tra phân giải Độ phân giải màn hình (Resolution)
# ==============================================================================

def test_resolve_resolution_pixels_and_text(resolver):
    """Kiểm tra nhận diện độ phân giải bằng cặp pixel, chuỗi px hoặc nhãn text."""
    # Qua cặp width x height
    assert resolver.resolve_resolution(width=1920, height=1080) == "FHD"
    assert resolver.resolve_resolution(width=1920, height=1200) == "WUXGA"
    assert resolver.resolve_resolution(width=2560, height=1440) == "2K"
    assert resolver.resolve_resolution(width=3840, height=2160) == "4K"

    # Qua chuỗi trong text
    assert resolver.resolve_resolution("Màn hình 15.6 inch 1920x1080 sáng đẹp") == "FHD"
    assert resolver.resolve_resolution("Màn hình 14 inch 2.8k oled") == "2.8K"
    assert resolver.resolve_resolution("Màn hình full hd chống chói") == "FHD"
    assert resolver.resolve_resolution("Macbook air màn liquid retina") == "Liquid Retina"


# ==============================================================================
# 7. Kiểm tra phân giải Hệ điều hành (OS)
# ==============================================================================

def test_resolve_os(resolver):
    """Kiểm tra nhận diện hệ điều hành."""
    assert resolver.resolve_os("Máy cài sẵn win 11 bản quyền") == "Windows 11 Home"
    assert resolver.resolve_os("Laptop cài sẵn win 11 pro mượt mà") == "Windows 11 Pro"
    assert resolver.resolve_os("Macbook đang chạy macos mới nhất") == "macOS"
    assert resolver.resolve_os("Máy mua freedos chưa cài win") == "Ubuntu / FreeDOS"


# ==============================================================================
# 8. Kiểm tra tương thích PySpark Patterns & Điều kiện
# ==============================================================================

def test_spark_patterns_and_conditions(resolver):
    """Kiểm tra danh sách Spark Java patterns và SQL conditions phục vụ Silver layer."""
    assert "brand" in resolver.spark_patterns
    assert "series" in resolver.spark_patterns
    assert len(resolver.spark_patterns["series"]) > 0

    brand_conds = resolver.get_series_to_brand_conditions()
    assert len(brand_conds) > 0
    # Cặp điều kiện có dạng (pattern, brand_name)
    pat, brand = brand_conds[0]
    assert isinstance(pat, str) and isinstance(brand, str)

    cat_conds = resolver.get_series_to_category_conditions()
    assert len(cat_conds) > 0
    pat_cat, cat = cat_conds[0]
    assert isinstance(pat_cat, str) and isinstance(cat, str)
