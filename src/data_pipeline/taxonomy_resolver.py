import os
import re
import json
import unicodedata
from typing import Optional, Dict, Any, List, Tuple


# Regex Unicode Word Boundaries:
# Java regex engine in Spark supports \p{L} (Unicode Letter) and \p{N} (Unicode Number).
# Python re supports [^\W_] or (?<!\w) / (?!\w).
SPARK_L = r"(?<![\p{L}\p{N}])"
SPARK_R = r"(?![\p{L}\p{N}])"
PYTHON_L = r"(?<![^\W_])"
PYTHON_R = r"(?![^\W_])"


def normalize_str(s: Optional[str]) -> str:
    """Chuẩn hóa chuỗi văn bản: NFC, lower, xóa khoảng trắng thừa."""
    if not s:
        return ""
    text = unicodedata.normalize("NFC", str(s))
    text = re.sub(r"[\u00A0\u2000-\u200B\u202F\u205F\u3000\uFEFF]", " ", text)
    text = text.lower()
    text = re.sub(r"\s+", " ", text).strip()
    return text


def build_alias_pattern(aliases: List[str], dialect: str = "spark") -> str:
    r"""
    Tự động xây dựng regex pattern từ danh sách alias:
    - Sắp xếp alias theo thứ tự độ dài giảm dần (ưu tiên khớp cụm từ dài nhất trước).
    - Thoát các ký tự đặc biệt trong regex (+, ., -, (, ), v.v.).
    - Chuẩn hóa khoảng trắng thành \s+.
    - Bọc bởi Unicode Word Boundary an toàn.
    """
    if not aliases:
        return ""

    # Sắp xếp alias giảm dần theo độ dài để tránh khớp nhầm tiền tố ngắn
    sorted_aliases = sorted(aliases, key=lambda a: len(a.strip()), reverse=True)

    escaped_patterns = []
    for alias in sorted_aliases:
        alias_clean = normalize_str(alias)
        if not alias_clean:
            continue
        # Thoát các ký tự đặc biệt trừ khoảng trắng
        parts = alias_clean.split(" ")
        escaped_parts = [re.escape(part) for part in parts if part]
        escaped_alias = r"\s+".join(escaped_parts)
        escaped_patterns.append(escaped_alias)

    if not escaped_patterns:
        return ""

    joined = "|".join(escaped_patterns)
    left_boundary = SPARK_L if dialect == "spark" else PYTHON_L
    right_boundary = SPARK_R if dialect == "spark" else PYTHON_R

    return f"{left_boundary}(?:{joined}){right_boundary}"


class TaxonomyResolver:
    """
    Bộ phân giải ngữ nghĩa và ánh xạ thực thể phần cứng / laptop
    dựa trên từ điển taxonomy_dictionary.json.
    """

    def __init__(self, dict_path: Optional[str] = None):
        self.dict_path = dict_path or os.getenv("TAXONOMY_DICT_PATH")
        if not self.dict_path:
            # Tìm đường dẫn mặc định từ root của project
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            default_path = os.path.join(base_dir, "docs", "taxonomy_dictionary.json")
            self.dict_path = default_path if os.path.exists(default_path) else "docs/taxonomy_dictionary.json"

        self.data: Dict[str, Any] = self._load_dictionary()
        self._build_lookup_tables()
        self._compile_regex_matchers()

    def _load_dictionary(self) -> Dict[str, Any]:
        """Đọc và giải mã dữ liệu JSON từ điển."""
        if not os.path.exists(self.dict_path):
            raise FileNotFoundError(f"Taxonomy dictionary not found at path: {self.dict_path}")
        with open(self.dict_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _build_lookup_tables(self):
        """Xây dựng các bảng tra cứu (lookup tables) từ điển sang Canonical Name và Metadata."""
        # 1. Brands
        self.brands_meta: Dict[str, Dict[str, Any]] = {}
        self.alias_to_brand: Dict[str, str] = {}
        for item in self.data.get("brands", []):
            c_name = item["canonical_name"]
            self.brands_meta[c_name.lower()] = item
            # Alias chính là canonical name
            self.alias_to_brand[normalize_str(c_name)] = c_name
            for alias in item.get("aliases", []):
                self.alias_to_brand[normalize_str(alias)] = c_name

        # 2. Series & Category
        self.series_meta: Dict[str, Dict[str, Any]] = {}
        self.alias_to_series: Dict[str, Dict[str, Any]] = {}
        for item in self.data.get("series", []):
            c_name = item["canonical_name"]
            self.series_meta[c_name.lower()] = item
            entry_info = {
                "canonical_name": c_name,
                "brand": item.get("brand"),
                "category": item.get("category"),
            }
            self.alias_to_series[normalize_str(c_name)] = entry_info
            for alias in item.get("aliases", []):
                self.alias_to_series[normalize_str(alias)] = entry_info

        # 3. CPU (brands & tiers)
        self.alias_to_cpu_brand: Dict[str, str] = {}
        cpu_section = self.data.get("cpu", {})
        for item in cpu_section.get("brands", []):
            c_name = item["canonical_name"]
            self.alias_to_cpu_brand[normalize_str(c_name)] = c_name
            for alias in item.get("aliases", []):
                self.alias_to_cpu_brand[normalize_str(alias)] = c_name

        self.alias_to_cpu_tier: Dict[str, Dict[str, Any]] = {}
        for item in cpu_section.get("tiers", []):
            c_name = item["canonical_name"]
            entry_info = {
                "canonical_name": c_name,
                "brand": item.get("brand")
            }
            self.alias_to_cpu_tier[normalize_str(c_name)] = entry_info
            for alias in item.get("aliases", []):
                self.alias_to_cpu_tier[normalize_str(alias)] = entry_info

        # 4. RAM
        ram_section = self.data.get("ram", {})
        self.alias_to_ram_type: Dict[str, str] = {}
        for item in ram_section.get("types", []):
            c_name = item["canonical_name"]
            self.alias_to_ram_type[normalize_str(c_name)] = c_name
            for alias in item.get("aliases", []):
                self.alias_to_ram_type[normalize_str(alias)] = c_name
        self.standard_ram_capacities: set = set(ram_section.get("standard_capacities_gb", []))

        # 5. Storage
        storage_section = self.data.get("storage", {})
        self.alias_to_storage_type: Dict[str, str] = {}
        for item in storage_section.get("types", []):
            c_name = item["canonical_name"]
            self.alias_to_storage_type[normalize_str(c_name)] = c_name
            for alias in item.get("aliases", []):
                self.alias_to_storage_type[normalize_str(alias)] = c_name
        self.standard_storage_capacities: set = set(storage_section.get("standard_capacities_gb", []))

        # 6. Resolution
        res_section = self.data.get("resolution", {})
        self.alias_to_resolution: Dict[str, str] = {}
        self.pixels_to_resolution: Dict[str, str] = {}
        for item in res_section.get("standards", []):
            c_name = item["canonical_name"]
            pixel_str = item.get("pixels", "")
            if pixel_str:
                self.pixels_to_resolution[pixel_str.lower().strip()] = c_name
                # chuẩn hóa dạng px có khoảng trắng: "1920 x 1080" -> "1920x1080"
                self.pixels_to_resolution[re.sub(r"\s+", "", pixel_str.lower())] = c_name

            self.alias_to_resolution[normalize_str(c_name)] = c_name
            for alias in item.get("aliases", []):
                norm_a = normalize_str(alias)
                self.alias_to_resolution[norm_a] = c_name
                if "x" in norm_a:
                    self.pixels_to_resolution[re.sub(r"\s+", "", norm_a)] = c_name

        # 7. GPU (dedicated & integrated)
        gpu_section = self.data.get("gpu", {})
        self.alias_to_gpu: Dict[str, Dict[str, Any]] = {}
        for item in gpu_section.get("dedicated_chips", []):
            c_name = item["canonical_name"]
            entry_info = {
                "canonical_name": c_name,
                "gpu_type": "discrete",
                "vram_gb": item.get("vram_gb")
            }
            self.alias_to_gpu[normalize_str(c_name)] = entry_info
            for alias in item.get("aliases", []):
                self.alias_to_gpu[normalize_str(alias)] = entry_info

        for item in gpu_section.get("integrated_chips", []):
            c_name = item["canonical_name"]
            entry_info = {
                "canonical_name": c_name,
                "gpu_type": "integrated",
                "vram_gb": None
            }
            self.alias_to_gpu[normalize_str(c_name)] = entry_info
            for alias in item.get("aliases", []):
                self.alias_to_gpu[normalize_str(alias)] = entry_info

        # 8. OS
        self.alias_to_os: Dict[str, str] = {}
        for item in self.data.get("os", []):
            c_name = item["canonical_name"]
            self.alias_to_os[normalize_str(c_name)] = c_name
            for alias in item.get("aliases", []):
                self.alias_to_os[normalize_str(alias)] = c_name

    def _compile_regex_matchers(self):
        """Biên dịch các Regex Matchers phục vụ cả PySpark (Java boundary) và Python (re module)."""
        # Thu thập danh sách alias cho từng thực thể
        brand_aliases = list(self.alias_to_brand.keys())
        series_aliases = list(self.alias_to_series.keys())
        cpu_tier_aliases = list(self.alias_to_cpu_tier.keys())
        cpu_brand_aliases = list(self.alias_to_cpu_brand.keys())
        gpu_aliases = list(self.alias_to_gpu.keys())
        res_aliases = list(self.alias_to_resolution.keys())
        os_aliases = list(self.alias_to_os.keys())

        # Spark patterns (chạy trên JVM qua PySpark rlike, regexp_extract)
        self.spark_patterns = {
            "brand": build_alias_pattern(brand_aliases, dialect="spark"),
            "series": build_alias_pattern(series_aliases, dialect="spark"),
            "cpu_tier": build_alias_pattern(cpu_tier_aliases, dialect="spark"),
            "cpu_brand": build_alias_pattern(cpu_brand_aliases, dialect="spark"),
            "gpu": build_alias_pattern(gpu_aliases, dialect="spark"),
            "resolution": build_alias_pattern(res_aliases, dialect="spark"),
            "os": build_alias_pattern(os_aliases, dialect="spark"),
        }

        # Python compiled re (dùng cho unit tests, UDF, hoặc xử lý pure Python)
        def compile_py(pattern: str) -> Optional[re.Pattern]:
            return re.compile(pattern, re.IGNORECASE | re.UNICODE) if pattern else None

        self.python_patterns = {
            "brand": compile_py(build_alias_pattern(brand_aliases, dialect="python")),
            "series": compile_py(build_alias_pattern(series_aliases, dialect="python")),
            "cpu_tier": compile_py(build_alias_pattern(cpu_tier_aliases, dialect="python")),
            "cpu_brand": compile_py(build_alias_pattern(cpu_brand_aliases, dialect="python")),
            "gpu": compile_py(build_alias_pattern(gpu_aliases, dialect="python")),
            "resolution": compile_py(build_alias_pattern(res_aliases, dialect="python")),
            "os": compile_py(build_alias_pattern(os_aliases, dialect="python")),
        }

    # ==========================================================================
    # Public Resolution Methods (Pure Python / UDF / Testing)
    # ==========================================================================

    def resolve_series(self, text: str) -> Optional[Dict[str, Any]]:
        """Nhận diện dòng máy (Series) và suy ra Hãng (Brand) cùng Phân khúc (Category)."""
        norm_text = normalize_str(text)
        matcher = self.python_patterns["series"]
        if not matcher or not norm_text:
            return None

        m = matcher.search(norm_text)
        if m:
            matched_alias = normalize_str(m.group(0))
            if matched_alias in self.alias_to_series:
                res = self.alias_to_series[matched_alias].copy()
                res["matched_text"] = m.group(0)
                return res
        return None

    def resolve_brand(self, text: str) -> Optional[str]:
        """Nhận diện trực tiếp thương hiệu từ văn bản."""
        norm_text = normalize_str(text)
        matcher = self.python_patterns["brand"]
        if not matcher or not norm_text:
            return None

        m = matcher.search(norm_text)
        if m:
            matched_alias = normalize_str(m.group(0))
            return self.alias_to_brand.get(matched_alias)
        return None

    def resolve_device(
        self,
        title: Optional[str],
        description: Optional[str] = "",
        pc_brand_code: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Cơ chế phân giải phân cấp (Hierarchical Inference Engine):
        1. Ưu tiên tìm Series từ Title, nếu không có tìm từ Description.
        2. Nếu Series tồn tại -> suy luận Canonical Brand và Category từ Series.
        3. Nếu Series không có -> tìm Brand trực tiếp từ Title, rồi Description.
        4. Fallback: Nếu Brand vẫn rỗng và pc_brand_code == 3 -> gán 'Apple'.
        5. Lấy cấu hình mặc định (default_os, default_cpu_brand) theo Hãng.
        """
        title_norm = normalize_str(title)
        desc_norm = normalize_str(description)

        series_info = None
        series_src = None

        # 1. Khớp Series
        if title_norm:
            series_info = self.resolve_series(title_norm)
            if series_info:
                series_src = "title"

        if not series_info and desc_norm:
            series_info = self.resolve_series(desc_norm)
            if series_info:
                series_src = "description"

        # 2. Suy luận Brand & Category
        canonical_brand = None
        canonical_series = None
        device_category = None
        brand_src = None

        if series_info:
            canonical_series = series_info["canonical_name"]
            canonical_brand = series_info["brand"]
            device_category = series_info["category"]
            brand_src = "inferred_from_series"
        else:
            # 3. Tìm Brand trực tiếp
            if title_norm:
                direct_b = self.resolve_brand(title_norm)
                if direct_b:
                    canonical_brand = direct_b
                    brand_src = "direct_title"

            if not canonical_brand and desc_norm:
                direct_b = self.resolve_brand(desc_norm)
                if direct_b:
                    canonical_brand = direct_b
                    brand_src = "direct_desc"

            # 4. Fallback Chợ Tốt pc_brand == 3 (Apple)
            if not canonical_brand and pc_brand_code == 3:
                canonical_brand = "Apple"
                brand_src = "pc_brand_code"

        # 5. Default Metadata theo Hãng
        default_os = None
        default_cpu_brand = None
        if canonical_brand:
            b_meta = self.brands_meta.get(canonical_brand.lower(), {})
            default_os = b_meta.get("default_os")
            default_cpu_brand = b_meta.get("default_cpu_brand")

        return {
            "canonical_brand": canonical_brand,
            "canonical_series": canonical_series,
            "device_category": device_category,
            "brand_source": brand_src,
            "series_source": series_src,
            "default_os": default_os,
            "default_cpu_brand": default_cpu_brand,
        }

    def resolve_cpu(self, text: str) -> Optional[Dict[str, Any]]:
        """Nhận diện CPU Tier và CPU Brand."""
        norm_text = normalize_str(text)
        if not norm_text:
            return None

        # 1. Khớp Tier
        matcher_tier = self.python_patterns["cpu_tier"]
        tier_info = None
        if matcher_tier:
            m = matcher_tier.search(norm_text)
            if m:
                matched_alias = normalize_str(m.group(0))
                tier_info = self.alias_to_cpu_tier.get(matched_alias)

        # 2. Khớp Vendor nếu Tier chưa có brand
        cpu_brand = None
        if tier_info:
            cpu_brand = tier_info.get("brand")
        else:
            matcher_brand = self.python_patterns["cpu_brand"]
            if matcher_brand:
                m_b = matcher_brand.search(norm_text)
                if m_b:
                    cpu_brand = self.alias_to_cpu_brand.get(normalize_str(m_b.group(0)))

        if tier_info or cpu_brand:
            return {
                "canonical_tier": tier_info["canonical_name"] if tier_info else None,
                "cpu_brand": cpu_brand
            }
        return None

    def resolve_gpu(self, text: str) -> Optional[Dict[str, Any]]:
        """Nhận diện GPU, phân loại Discrete / Integrated và VRAM."""
        norm_text = normalize_str(text)
        matcher = self.python_patterns["gpu"]
        if not matcher or not norm_text:
            return None

        m = matcher.search(norm_text)
        if m:
            matched_alias = normalize_str(m.group(0))
            if matched_alias in self.alias_to_gpu:
                return self.alias_to_gpu[matched_alias].copy()
        return None

    def resolve_resolution(
        self,
        text: Optional[str] = None,
        width: Optional[int] = None,
        height: Optional[int] = None
    ) -> Optional[str]:
        """
        Nhận diện độ phân giải màn hình chuẩn hóa:
        - Ưu tiên cặp pixel width x height.
        - Khớp qua văn bản / alias (fhd, 2k, wuxga, retina...).
        """
        # 1. Kiểm tra cặp pixel
        if width and height:
            px_key = f"{width}x{height}"
            if px_key in self.pixels_to_resolution:
                return self.pixels_to_resolution[px_key]

        if not text:
            return None

        norm_text = normalize_str(text)

        # 2. Kiểm tra chuỗi pixel trong text (vd 1920x1080)
        px_match = re.search(r"(\d{3,4})\s*[x*xX]\s*(\d{3,4})", norm_text)
        if px_match:
            px_key = f"{px_match.group(1)}x{px_match.group(2)}"
            if px_key in self.pixels_to_resolution:
                return self.pixels_to_resolution[px_key]

        # 3. Khớp alias văn bản
        matcher = self.python_patterns["resolution"]
        if matcher:
            m = matcher.search(norm_text)
            if m:
                matched_alias = normalize_str(m.group(0))
                return self.alias_to_resolution.get(matched_alias)

        return None

    def resolve_os(self, text: str) -> Optional[str]:
        """Nhận diện hệ điều hành từ văn bản."""
        norm_text = normalize_str(text)
        matcher = self.python_patterns["os"]
        if not matcher or not norm_text:
            return None

        m = matcher.search(norm_text)
        if m:
            matched_alias = normalize_str(m.group(0))
            return self.alias_to_os.get(matched_alias)
        return None

    # ==========================================================================
    # PySpark Pipeline Compatibility Helpers
    # ==========================================================================

    def get_series_canonical_conditions(self) -> List[Tuple[str, str]]:
        """
        Trả về danh sách các cặp (series_regex, canonical_series_name).
        Được sắp xếp sao cho dòng máy có alias dài nhất đứng trước (tránh khớp nhầm).
        """
        items = sorted(
            self.data.get("series", []),
            key=lambda x: max([len(a) for a in x.get("aliases", []) + [x.get("canonical_name", "")]] or [0]),
            reverse=True
        )
        conditions = []
        for item in items:
            c_name = item.get("canonical_name")
            aliases = item.get("aliases", []) + [c_name]
            pat = build_alias_pattern(aliases, dialect="spark")
            if pat and c_name:
                conditions.append((pat, c_name))
        return conditions

    def get_series_to_brand_conditions(self) -> List[Tuple[str, str]]:
        """
        Trả về danh sách các cặp (series_regex, canonical_brand)
        phục vụ mệnh đề Spark SQL when(col.rlike(series_regex), lit(canonical_brand)).
        """
        items = sorted(
            self.data.get("series", []),
            key=lambda x: max([len(a) for a in x.get("aliases", []) + [x.get("canonical_name", "")]] or [0]),
            reverse=True
        )
        conditions = []
        for item in items:
            brand = item.get("brand")
            aliases = item.get("aliases", []) + [item.get("canonical_name")]
            pat = build_alias_pattern(aliases, dialect="spark")
            if pat and brand:
                conditions.append((pat, brand))
        return conditions

    def get_series_to_category_conditions(self) -> List[Tuple[str, str]]:
        """
        Trả về danh sách các cặp (series_regex, category)
        phục vụ mệnh đề Spark SQL when(col.rlike(series_regex), lit(category)).
        """
        items = sorted(
            self.data.get("series", []),
            key=lambda x: max([len(a) for a in x.get("aliases", []) + [x.get("canonical_name", "")]] or [0]),
            reverse=True
        )
        conditions = []
        for item in items:
            cat = item.get("category")
            aliases = item.get("aliases", []) + [item.get("canonical_name")]
            pat = build_alias_pattern(aliases, dialect="spark")
            if pat and cat:
                conditions.append((pat, cat))
        return conditions

    def get_brand_direct_conditions(self) -> List[Tuple[str, str]]:
        """
        Trả về danh sách các cặp (brand_regex, canonical_brand)
        phục vụ nhận diện thương hiệu trực tiếp.
        """
        items = sorted(
            self.data.get("brands", []),
            key=lambda x: max([len(a) for a in x.get("aliases", []) + [x.get("canonical_name", "")]] or [0]),
            reverse=True
        )
        conditions = []
        for item in items:
            c_name = item.get("canonical_name")
            aliases = item.get("aliases", []) + [c_name]
            pat = build_alias_pattern(aliases, dialect="spark")
            if pat and c_name:
                conditions.append((pat, c_name))
        return conditions

    def get_resolution_conditions(self) -> List[Tuple[str, str]]:
        """
        Trả về danh sách các cặp (resolution_alias_regex, canonical_resolution).
        """
        items = sorted(
            self.data.get("resolution", {}).get("standards", []),
            key=lambda x: max([len(a) for a in x.get("aliases", []) + [x.get("canonical_name", "")]] or [0]),
            reverse=True
        )
        conditions = []
        for item in items:
            c_name = item.get("canonical_name")
            aliases = item.get("aliases", []) + [c_name]
            pat = build_alias_pattern(aliases, dialect="spark")
            if pat and c_name:
                conditions.append((pat, c_name))
        return conditions

    def get_gpu_vram_conditions(self) -> List[Tuple[str, int]]:
        """
        Trả về danh sách các cặp (gpu_regex, vram_gb) cho các GPU rời có định nghĩa vram_gb.
        """
        items = sorted(
            self.data.get("gpu", {}).get("dedicated_chips", []),
            key=lambda x: max([len(a) for a in x.get("aliases", []) + [x.get("canonical_name", "")]] or [0]),
            reverse=True
        )
        conditions = []
        for item in items:
            vram = item.get("vram_gb")
            if vram is not None:
                aliases = item.get("aliases", []) + [item.get("canonical_name")]
                pat = build_alias_pattern(aliases, dialect="spark")
                if pat:
                    conditions.append((pat, vram))
        return conditions
