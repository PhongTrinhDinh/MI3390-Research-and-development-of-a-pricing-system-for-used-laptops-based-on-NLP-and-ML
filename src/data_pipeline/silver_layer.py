import os
from src.utils.logger import get_logger
from src.utils.spark_utils import get_spark_session
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    when,
    coalesce,
    regexp_replace,
    regexp_extract,
    trim,
    lower,
    concat_ws,
    round as spark_round,
    length,
    substring,
    udf,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

from src.data_pipeline.taxonomy_resolver import TaxonomyResolver
from src.data_pipeline.silver_patterns import (
    normalize_text_column,
    extract_safe,
    extract_pref,
    BRAND_DIRECT_PATTERN,
    PRODUCT_LINE_PATTERN,
    BRAND_FROM_LINE_CONDITIONS,
    INTEL_MODEL_PATTERN,
    INTEL_GEN_PHRASE_PATTERN,
    INTEL_ULTRA_PATTERN,
    AMD_MODEL_PATTERN,
    APPLE_M_PATTERN,
    GPU_DISCRETE_PATTERN,
    GPU_INTEGRATED_PATTERN,
    SCREEN_SIZE_PATTERN,
    RESOLUTION_NUM_PATTERN,
    RESOLUTION_TEXT_PATTERN,
    TOUCH_POS_PATTERN,
    TOUCH_NEG_PATTERN,
    NEW_SEAL_PATTERN,
    LIKE_NEW_PATTERN,
    CONDITION_PCT_PATTERN,
    ORIGIN_COUNTRY_PATTERN,
    ORIGIN_TYPE_PATTERN,
    WARRANTY_MONTHS_PATTERN,
    WARRANTY_YEARS_PATTERN,
    WARRANTY_EXPIRED_PATTERN,
    WARRANTY_TYPE_PATTERN,
    UPGRADE_PATTERN,
    REPAIR_ORIGINAL_PATTERN,
    REPAIR_FIXED_PATTERN,
    YEAR_PRODUCTION_PATTERN,
    BUNDLES_PATTERN,
)

logger = get_logger(__name__, log_file="logs/etl_pipeline/silver.log")

_TAXONOMY_RESOLVER = None


def get_taxonomy_resolver() -> TaxonomyResolver:
    """Khởi tạo hoặc tái sử dụng instance TaxonomyResolver trên worker (Lazy Singleton)."""
    global _TAXONOMY_RESOLVER
    if _TAXONOMY_RESOLVER is None:
        _TAXONOMY_RESOLVER = TaxonomyResolver()
    return _TAXONOMY_RESOLVER


def _safe_cast_float(str_col):
    """Ép kiểu chuỗi sang float an toàn, trả về NULL nếu chuỗi rỗng hoặc NULL."""
    return when((str_col.isNotNull()) & (trim(str_col) != ""), str_col.cast("float")).otherwise(lit(None))


# Định nghĩa Spark UDFs để giải quyết ánh xạ Taxonomy trên Python worker
DEVICE_TAXONOMY_SCHEMA = StructType([
    StructField("canonical_brand", StringType(), True),
    StructField("canonical_series", StringType(), True),
    StructField("device_category", StringType(), True),
])


@udf(returnType=DEVICE_TAXONOMY_SCHEMA)
def resolve_device_taxonomy_udf(title, desc, pc_brand_code):
    resolver = get_taxonomy_resolver()
    res = resolver.resolve_device(title, desc, pc_brand_code)
    return (
        res.get("canonical_brand"),
        res.get("canonical_series"),
        res.get("device_category"),
    )


@udf(returnType=IntegerType())
def resolve_gpu_vram_udf(gpu_model):
    if not gpu_model:
        return None
    resolver = get_taxonomy_resolver()
    res = resolver.resolve_gpu(str(gpu_model))
    if res and res.get("vram_gb") is not None:
        return int(res["vram_gb"])
    return None


@udf(returnType=StringType())
def resolve_canonical_resolution_udf(title, desc, res_text):
    resolver = get_taxonomy_resolver()
    if res_text:
        res = resolver.resolve_resolution(text=str(res_text))
        if res:
            return res
    combined = f"{title or ''} {desc or ''}"
    res = resolver.resolve_resolution(text=combined)
    return res if res else "unknown"


def process_silver_layer(spark: SparkSession = None):
    is_owner = False
    if spark is None:
        spark = get_spark_session("SilverLayer_Processing")
        is_owner = True

    # Cấu hình chống tràn bộ nhớ bytecode Janino compiler cho Spark SQL
    spark.conf.set("spark.sql.codegen.wholeStage", "false")

    bronze_dir = os.getenv("DATALAKE_BRONZE_DIR", "./data/raw/bronze_layer")
    silver_dir = os.getenv("DATALAKE_SILVER_DIR", "./data/processed/silver_layer")

    logger.info(f"Reading data from Bronze: {bronze_dir}")

    try:
        df_bronze = spark.read.parquet(bronze_dir).dropDuplicates(["list_id"])

        # 1. Ép kiểu và chọn lọc dữ liệu từ Bronze
        df_flat = df_bronze.select(
            col("list_id").cast("string").alias("post_id"),
            col("title"),
            col("description"),
            col("price").cast("double").alias("target_price"),

            # Thông số dạng chuỗi từ nguồn
            col("cpu").alias("raw_cpu"),
            col("ram").alias("raw_ram"),
            col("hard_drive").alias("raw_hard_drive"),
            col("hard_drive_type").alias("raw_hard_drive_type"),

            # Mã số phân loại
            col("pc_brand").cast("integer"),
            col("pc_cpu").cast("integer"),
            col("pc_ram").cast("integer"),
            col("laptop_screen_size").cast("integer"),
            col("elt_condition").cast("integer"),
            col("elt_origin").cast("integer"),
            col("elt_warranty").cast("integer"),

            # Thông tin địa lý & người bán
            col("region_name"),
            col("ward_name"),
            col("seller_rating").cast("double"),
            col("seller_sold_count").cast("integer"),

            # Metadata
            col("crawled_at").cast("timestamp"),
            col("url")
        )

        # 2. Xử lý giá trị thiếu (Missing values) chuẩn ML:
        df_clean = df_flat.withColumn(
            "is_seller_rating_missing",
            when(col("seller_rating").isNull(), 1).otherwise(0)
        ).fillna({
            "elt_warranty": -1,
            "seller_sold_count": 0
        })

        # 3. Chuẩn hóa văn bản: NFC, NBSP, lower, gộp khoảng trắng
        df_norm = df_clean \
            .withColumn("title_norm", normalize_text_column(col("title"))) \
            .withColumn("desc_norm", normalize_text_column(col("description")))

        title_c = col("title_norm")
        desc_c = col("desc_norm")

        # ----------------------------------------------------------------------
        # 4. Trích xuất & Suy luận thực thể (Entity Inference & Standardization)
        # ----------------------------------------------------------------------

        # 4.1. Hãng (Brand), Dòng máy (Series), Phân khúc (Category) & Model ID
        device_meta = resolve_device_taxonomy_udf(col("title"), col("description"), col("pc_brand"))
        canonical_brand_val = device_meta.getField("canonical_brand")
        canonical_series_val = device_meta.getField("canonical_series")
        device_category_val = device_meta.getField("device_category")

        # Regex NLP Fallback cho dòng máy và hãng
        prod_line_raw = extract_pref(title_c, desc_c, PRODUCT_LINE_PATTERN, 0)
        prod_line_val = coalesce(canonical_series_val, prod_line_raw)

        brand_direct_val = extract_pref(title_c, desc_c, BRAND_DIRECT_PATTERN, 0)
        brand_inferred = lit(None)
        for pattern_cond, brand_label in reversed(BRAND_FROM_LINE_CONDITIONS):
            brand_inferred = when(prod_line_val.rlike(pattern_cond), lit(brand_label)).otherwise(brand_inferred)

        brand_pc_code = when(col("pc_brand") == 3, lit("apple")).otherwise(lit(None))
        brand_final = coalesce(lower(canonical_brand_val), brand_direct_val, brand_inferred, brand_pc_code)
        # Chốt chặn chuẩn hóa: hợp nhất 'macbook' về thương hiệu 'apple'
        brand_final = when(lower(brand_final) == "macbook", lit("apple")).otherwise(coalesce(brand_final, lit("unknown")))

        # Phân loại rõ: có tên dòng nhưng chưa map -> 'other', hoàn toàn thiếu -> 'unknown'
        product_line_final = when(
            canonical_series_val.isNotNull(), canonical_series_val
        ).when(
            prod_line_raw.isNotNull(), lit("other")
        ).otherwise(lit("unknown"))

        device_category_final = when(
            device_category_val.isNotNull(), device_category_val
        ).when(
            prod_line_raw.isNotNull(), lit("other")
        ).otherwise(lit("unknown"))

        # Tạo định danh Model ID chuẩn hóa
        brand_clean_slug = regexp_replace(lower(brand_final), r"[^a-z0-9]+", "-")
        line_clean_slug = regexp_replace(lower(coalesce(canonical_series_val, prod_line_val, lit("unknown"))), r"[^a-z0-9]+", "-")
        model_id_val = when(
            (brand_final == "unknown") & (product_line_final == "unknown"),
            lit("unknown")
        ).otherwise(concat_ws("-", brand_clean_slug, line_clean_slug))

        # 4.2. CPU: Chuẩn hóa Model, tách CPU Family, suy luận CPU Gen & CPU Vendor
        intel_m = extract_pref(title_c, desc_c, INTEL_MODEL_PATTERN, 1)
        intel_u = extract_pref(title_c, desc_c, INTEL_ULTRA_PATTERN, 0)
        amd_m = extract_pref(title_c, desc_c, AMD_MODEL_PATTERN, 1)
        apple_m = extract_pref(title_c, desc_c, APPLE_M_PATTERN, 1)

        cpu_model_raw = coalesce(intel_m, intel_u, amd_m, apple_m)
        # Chuẩn hóa format CPU Model:
        # Intel: i5 1135g7 / i5-1135g7 -> i5-1135g7
        # AMD: r5-5600h / r5 5600h -> ryzen 5 5600h
        # Apple: m1max -> m1 max, m3pro -> m3 pro
        cpu_model_clean = regexp_replace(cpu_model_raw, r"^core\s*", "")
        cpu_model_clean = regexp_replace(cpu_model_clean, r"^i([3579])[\s-]?(\d{3,5}[a-z0-9]*)", "i$1-$2")
        cpu_model_clean = regexp_replace(cpu_model_clean, r"^r([3579])[- ]?", "ryzen $1 ")
        cpu_model_clean = regexp_replace(cpu_model_clean, r"^m([1-4])(pro|max|ultra)", "m$1 $2")
        cpu_model_val = trim(regexp_replace(cpu_model_clean, r"\s+", " "))

        # Tách CPU Family
        cpu_family_val = (
            when(cpu_model_val.rlike(r"^i3-|\bcore\s*i3\b"), lit("core-i3"))
            .when(cpu_model_val.rlike(r"^i5-|\bcore\s*i5\b"), lit("core-i5"))
            .when(cpu_model_val.rlike(r"^i7-|\bcore\s*i7\b"), lit("core-i7"))
            .when(cpu_model_val.rlike(r"^i9-|\bcore\s*i9\b"), lit("core-i9"))
            .when(cpu_model_val.rlike(r"ultra"), lit("core-ultra"))
            .when(cpu_model_val.rlike(r"ryzen\s*3|^r3"), lit("ryzen-3"))
            .when(cpu_model_val.rlike(r"ryzen\s*5|^r5"), lit("ryzen-5"))
            .when(cpu_model_val.rlike(r"ryzen\s*7|^r7"), lit("ryzen-7"))
            .when(cpu_model_val.rlike(r"ryzen\s*9|^r9"), lit("ryzen-9"))
            .when(cpu_model_val.rlike(r"^m[1-4]"), lit("apple-m"))
            .when(title_c.rlike(r"\bceleron\b|\bpentium\b"), lit("intel-entry"))
            .otherwise(lit("unknown"))
        )

        # Suy luận CPU Gen:
        # Intel: lấy 2 số đầu nếu dài 5 số hoặc đầu 10-15, ngược lại lấy 1 số đầu
        intel_sku_digits = regexp_extract(cpu_model_val, r"i[3579]-?(\d{4,5})", 1)
        gen_intel = when(
            intel_sku_digits != "",
            when(
                (length(intel_sku_digits) == 5) | (substring(intel_sku_digits, 1, 2).isin(["10", "11", "12", "13", "14", "15"])),
                substring(intel_sku_digits, 1, 2).cast("integer")
            ).otherwise(substring(intel_sku_digits, 1, 1).cast("integer"))
        ).otherwise(lit(None))

        # AMD Ryzen: 4 chữ số -> số đầu tiên là gen (ví dụ: 4700u -> gen 4, 5600h -> gen 5, 7840hs -> gen 7)
        amd_sku_digits = regexp_extract(cpu_model_val, r"ryzen\s*[3579]\s*(?:pro\s*)?(\d)\d{3}", 1)
        gen_amd = when(amd_sku_digits != "", amd_sku_digits.cast("integer")).otherwise(lit(None))

        # Apple: M1 -> 1, M2 -> 2, M3 -> 3, M4 -> 4
        apple_m_digit = regexp_extract(cpu_model_val, r"m([1-4])", 1)
        gen_apple = when(apple_m_digit != "", apple_m_digit.cast("integer")).otherwise(lit(None))

        # Ultra: thế hệ tương đương 14
        gen_ultra = when(cpu_model_val.rlike(r"ultra"), lit(14)).otherwise(lit(None))
        gen_phrase = extract_pref(title_c, desc_c, INTEL_GEN_PHRASE_PATTERN, 1).cast("integer")
        cpu_gen_val = coalesce(gen_intel, gen_amd, gen_apple, gen_ultra, gen_phrase)

        # Suy luận CPU Vendor
        cpu_vendor_val = (
            when(cpu_family_val.isin(["core-i3", "core-i5", "core-i7", "core-i9", "core-ultra", "intel-entry"]), lit("intel"))
            .when(cpu_family_val.isin(["ryzen-3", "ryzen-5", "ryzen-7", "ryzen-9"]), lit("amd"))
            .when((cpu_family_val == "apple-m") | (brand_final == "apple"), lit("apple"))
            .when(intel_m.isNotNull() | intel_u.isNotNull(), lit("intel"))
            .when(amd_m.isNotNull(), lit("amd"))
            .otherwise(lit("unknown"))
        )

        # 4.3. GPU: Tách GPU Family, GPU Tier, phân loại GPU Type và cờ suy luận
        gpu_disc = extract_pref(title_c, desc_c, GPU_DISCRETE_PATTERN, 1)
        gpu_integ = extract_pref(title_c, desc_c, GPU_INTEGRATED_PATTERN, 1)
        gpu_model_raw = coalesce(gpu_disc, gpu_integ)
        gpu_model_clean = regexp_replace(gpu_model_raw, r"^(rtx|gtx|mx)(\d{3,4})", "$1 $2")
        gpu_model_clean = regexp_replace(gpu_model_clean, r"(\d{3,4})(ti|super)", "$1 $2")
        gpu_model_clean = regexp_replace(gpu_model_clean, r"uhd\s*graphiics", "uhd graphics")
        gpu_model_clean = regexp_replace(gpu_model_clean, r"intel\s*uhd", "uhd")
        gpu_model_val = trim(regexp_replace(gpu_model_clean, r"\s+", " "))

        # Họ GPU (gpu_family)
        gpu_family_val = (
            when(gpu_model_raw.rlike(r"rtx"), lit("rtx"))
            .when(gpu_model_raw.rlike(r"gtx"), lit("gtx"))
            .when(gpu_model_raw.rlike(r"mx\s*\d+"), lit("mx"))
            .when(gpu_model_raw.rlike(r"quadro|rtx\s*a"), lit("quadro_workstation"))
            .when(gpu_model_raw.rlike(r"iris\s*xe"), lit("iris_xe"))
            .when(gpu_model_raw.rlike(r"uhd"), lit("intel_uhd"))
            .when(gpu_model_raw.rlike(r"intel\s*hd"), lit("intel_hd"))
            .when(gpu_model_raw.rlike(r"radeon"), lit("radeon"))
            .when((brand_final == "apple") | (cpu_vendor_val == "apple"), lit("apple_integrated"))
            .otherwise(lit("unknown"))
        )

        # Cấp bậc GPU (gpu_tier)
        gpu_tier_raw = regexp_extract(gpu_model_raw, r"(\d{3,4})", 1)
        gpu_tier_val = when(
            gpu_tier_raw.isin(["1050", "1650", "1660", "2060", "2070", "3050", "3060", "3070", "3080", "4050", "4060", "4070", "4080", "520", "530", "620", "630"]),
            gpu_tier_raw
        ).when(gpu_tier_raw != "", lit("other")).otherwise(lit("unknown"))

        # Phân loại GPU Type: Apple -> integrated, Gaming -> discrete (kèm cờ suy luận)
        is_gaming_line = prod_line_val.rlike(r"rog|tuf|legion|loq|nitro|predator|alienware|victus|omen")
        is_apple = (brand_final == "apple") | (cpu_vendor_val == "apple")

        gpu_type_val = (
            when(is_apple, lit("integrated"))
            .when(gpu_disc.isNotNull(), lit("discrete"))
            .when(gpu_integ.isNotNull(), lit("integrated"))
            .when(is_gaming_line, lit("discrete"))
            .otherwise(lit("unknown"))
        )

        is_gpu_inferred = when(
            (gpu_disc.isNull() & gpu_integ.isNull()) & (is_apple | is_gaming_line),
            1
        ).otherwise(0)

        gpu_vram_val = resolve_gpu_vram_udf(gpu_model_val)

        # 4.4. RAM: Lọc tập giá trị hợp lệ loại trừ nhiễu 1, 3, 7, 10 GB
        ram_str_col = regexp_extract(lower(coalesce(col("raw_ram"), lit(""))), r"(\d+)\s*gb", 1)
        ram_str_text = regexp_extract(title_c, r"\b(\d{1,2})\s*gb\b", 1)
        ram_str = when(ram_str_col != "", ram_str_col).when(ram_str_text != "", ram_str_text).otherwise(lit(None))
        ram_raw_num = _safe_cast_float(ram_str).cast("double")
        # Tập hợp RAM chuẩn thị trường
        ram_gb_val = when(
            ram_raw_num.isin([4.0, 8.0, 12.0, 16.0, 24.0, 32.0, 48.0, 64.0, 128.0]),
            ram_raw_num
        ).otherwise(lit(None))

        # 4.5. Storage: Tách ssd_gb và hdd_gb, gắn cờ has_dual_drive
        ssd_tb_text = _safe_cast_float(regexp_extract(title_c, r"(\d+(?:\.\d+)?)\s*tb\s*(?:ssd|nvme)", 1)) * 1024.0
        ssd_gb_text = _safe_cast_float(regexp_extract(title_c, r"\b(128|256|512|1000|1024)\s*gb\s*(?:ssd|nvme)", 1))
        ssd_text = coalesce(ssd_tb_text, ssd_gb_text)

        hdd_tb_text = _safe_cast_float(regexp_extract(title_c, r"(\d+(?:\.\d+)?)\s*tb\s*(?:hdd|ổ\s*cơ)", 1)) * 1024.0
        hdd_gb_text = _safe_cast_float(regexp_extract(title_c, r"\b(250|320|500|1000|1024)\s*gb\s*(?:hdd|ổ\s*cơ)", 1))
        hdd_text = coalesce(hdd_tb_text, hdd_gb_text)

        drive_tb_col = _safe_cast_float(regexp_extract(lower(coalesce(col("raw_hard_drive"), lit(""))), r"(\d+(?:\.\d+)?)\s*tb", 1)) * 1024.0
        drive_gb_col = _safe_cast_float(regexp_extract(lower(coalesce(col("raw_hard_drive"), lit(""))), r"(\d+)\s*gb", 1))
        drive_total_col = coalesce(drive_tb_col, drive_gb_col)

        drive_type_col = lower(coalesce(col("raw_hard_drive_type"), lit("")))
        is_ssd_col = drive_type_col.contains("ssd") | drive_type_col.contains("nvme") | title_c.contains("ssd") | title_c.contains("nvme")
        is_hdd_col = drive_type_col.contains("hdd") | title_c.contains("hdd")

        ssd_gb_val = coalesce(
            ssd_text,
            when(is_ssd_col, drive_total_col),
            when(drive_total_col.isNotNull() & (~is_hdd_col), drive_total_col),
            lit(0.0)
        ).cast("double")

        hdd_gb_val = coalesce(
            hdd_text,
            when(is_hdd_col & (~is_ssd_col), drive_total_col),
            lit(0.0)
        ).cast("double")

        has_dual_drive_val = when((ssd_gb_val > 0) & (hdd_gb_val > 0), 1).otherwise(0)

        # 4.6. Screen Size & Canonical Resolution
        screen_raw = extract_pref(title_c, desc_c, SCREEN_SIZE_PATTERN, 1)
        screen_standardized = regexp_replace(screen_raw, ",", ".").cast("double")
        screen_size_inch = spark_round(
            when((screen_standardized >= 10.0) & (screen_standardized <= 18.5), screen_standardized).otherwise(lit(None)),
            1
        )

        res_num_w = extract_pref(title_c, desc_c, RESOLUTION_NUM_PATTERN, 1).cast("integer")
        res_num_h = extract_pref(title_c, desc_c, RESOLUTION_NUM_PATTERN, 2).cast("integer")
        res_text = extract_pref(title_c, desc_c, RESOLUTION_TEXT_PATTERN, 1)

        res_pixel_standard = (
            when((res_num_w == 1920) & (res_num_h == 1080), lit("FHD"))
            .when((res_num_w == 1920) & (res_num_h == 1200), lit("WUXGA"))
            .when((res_num_w == 2560) & (res_num_h == 1440), lit("2K"))
            .when((res_num_w == 2560) & (res_num_h == 1600), lit("Liquid Retina"))
            .when((res_num_w == 2880) & (res_num_h == 1800), lit("2.8K"))
            .when((res_num_w == 3072) & (res_num_h == 1920), lit("3K"))
            .when((res_num_w == 3840) & ((res_num_h == 2160) | (res_num_h == 2400)), lit("4K"))
            .otherwise(lit(None))
        )
        resolution_val = coalesce(
            res_pixel_standard,
            resolve_canonical_resolution_udf(col("title"), col("description"), res_text),
            lit("unknown")
        )

        # 4.7. Touch Sensitive (Chuyển thành phân loại 3 trạng thái: touch, no_touch, unknown)
        touch_pos = extract_pref(title_c, desc_c, TOUCH_POS_PATTERN, 0)
        touch_neg = extract_pref(title_c, desc_c, TOUCH_NEG_PATTERN, 0)
        touch_sensitive_val = (
            when(touch_pos.isNotNull(), lit("touch"))
            .when(touch_neg.isNotNull(), lit("no_touch"))
            .otherwise(lit("unknown"))
        )

        # 4.8. Condition, Origin, Warranty, Bundle, Repair
        like_new_raw = extract_pref(title_c, desc_c, LIKE_NEW_PATTERN, 0)
        new_seal_raw = extract_pref(title_c, desc_c, NEW_SEAL_PATTERN, 0)
        cond_pct_raw = extract_pref(title_c, desc_c, CONDITION_PCT_PATTERN, 1).cast("integer")
        is_like_new_val = when(
            like_new_raw.isNotNull() | new_seal_raw.isNotNull() | (cond_pct_raw >= 98),
            1
        ).otherwise(0)

        origin_country_raw = extract_pref(title_c, desc_c, ORIGIN_COUNTRY_PATTERN, 1)
        origin_raw = extract_pref(title_c, desc_c, ORIGIN_TYPE_PATTERN, 1)
        origin_type_val = (
            when(origin_raw.rlike(r"chính\s*hãng|fpt|tgdd|thế\s*giới\s*di\s*động|viettel|vn/a"), lit("chinh_hang"))
            .when(origin_raw.rlike(r"xách\s*tay|hàng\s*bãi|nhập\s*khẩu|ll/a|j/a|kh/a|za/a") | origin_country_raw.isNotNull(), lit("xach_tay"))
            .when(col("elt_origin") == 1, lit("chinh_hang"))
            .when(col("elt_origin").isin(8, 5, 7, 10, 6, 3, 9, 4), lit("xach_tay"))
            .otherwise(lit("unknown"))
        )

        w_m = extract_pref(title_c, desc_c, WARRANTY_MONTHS_PATTERN, 1).cast("integer")
        w_y = extract_pref(title_c, desc_c, WARRANTY_YEARS_PATTERN, 1).cast("integer")
        w_exp = extract_pref(title_c, desc_c, WARRANTY_EXPIRED_PATTERN, 0)
        w_type_raw = extract_pref(title_c, desc_c, WARRANTY_TYPE_PATTERN, 1)
        has_warranty_val = (
            when(w_exp.isNotNull(), 0)
            .when((w_m.isNotNull()) & (w_m > 0), 1)
            .when((w_y.isNotNull()) & (w_y > 0), 1)
            .when(w_type_raw.isNotNull(), 1)
            .when(col("elt_warranty") > 1, 1)
            .when(title_c.rlike(r"bảo\s*hành|bh\s*\d+|applecare"), 1)
            .otherwise(0)
        )

        is_upgraded_val = when(extract_pref(title_c, desc_c, UPGRADE_PATTERN, 0).isNotNull(), 1).otherwise(0)
        rep_fixed = extract_pref(title_c, desc_c, REPAIR_FIXED_PATTERN, 0)
        is_repaired_val = when(rep_fixed.isNotNull(), 1).otherwise(0)

        year_prod_raw = extract_pref(title_c, desc_c, YEAR_PRODUCTION_PATTERN, 1).cast("integer")
        curr_yr = 2026
        year_production_val = when(
            (year_prod_raw >= 2010) & (year_prod_raw <= curr_yr + 1),
            year_prod_raw
        ).otherwise(lit(None))

        bundles_val = extract_safe(desc_c, BUNDLES_PATTERN, 1)
        has_bundle_val = when(
            (bundles_val.isNotNull() & (trim(bundles_val) != ""))
            | desc_c.rlike(r"tặng|kèm\s*(?:chuột|balo|túi|sạc|cáp)"),
            1
        ).otherwise(0)

        # 4.9. Địa danh (Gom thành Region Group giải quyết cardinality của ward)
        reg_lower = lower(coalesce(col("region_name"), lit("")))
        region_group_val = (
            when(reg_lower.contains("hồ chí minh") | reg_lower.contains("tp hcm") | reg_lower.contains("sài gòn"), lit("hcm"))
            .when(reg_lower.contains("hà nội"), lit("hanoi"))
            .when(reg_lower.contains("đà nẵng"), lit("da_nang"))
            .when(reg_lower.contains("hải phòng") | reg_lower.contains("cần thơ") | reg_lower.contains("bình dương") | reg_lower.contains("đồng nai"), lit("tier_2"))
            .otherwise(lit("other"))
        )

        # 5. Gán toàn bộ đặc trưng chuẩn hóa vào DataFrame
        df_features = df_norm \
            .withColumn("product_line", product_line_final) \
            .withColumn("canonical_series", canonical_series_val) \
            .withColumn("device_category", device_category_final) \
            .withColumn("brand_name", brand_final) \
            .withColumn("canonical_brand", canonical_brand_val) \
            .withColumn("model_id", model_id_val) \
            .withColumn("cpu_vendor", cpu_vendor_val) \
            .withColumn("cpu_family", cpu_family_val) \
            .withColumn("cpu_model", cpu_model_val) \
            .withColumn("cpu_gen", cpu_gen_val) \
            .withColumn("gpu_type", gpu_type_val) \
            .withColumn("gpu_family", gpu_family_val) \
            .withColumn("gpu_tier", gpu_tier_val) \
            .withColumn("gpu_model", gpu_model_val) \
            .withColumn("is_gpu_inferred", is_gpu_inferred) \
            .withColumn("gpu_vram_gb", gpu_vram_val) \
            .withColumn("ram_gb", ram_gb_val) \
            .withColumn("ssd_gb", ssd_gb_val) \
            .withColumn("hdd_gb", hdd_gb_val) \
            .withColumn("has_dual_drive", has_dual_drive_val) \
            .withColumn("screen_size_inch", screen_size_inch) \
            .withColumn("resolution", resolution_val) \
            .withColumn("canonical_resolution", resolution_val) \
            .withColumn("touch_sensitive", touch_sensitive_val) \
            .withColumn("is_like_new", is_like_new_val) \
            .withColumn("origin_type", origin_type_val) \
            .withColumn("has_warranty", has_warranty_val) \
            .withColumn("is_upgraded", is_upgraded_val) \
            .withColumn("is_repaired", is_repaired_val) \
            .withColumn("year_production", year_production_val) \
            .withColumn("has_bundle", has_bundle_val) \
            .withColumn("region_group", region_group_val)

        # 6. Loại bỏ các cột văn bản gốc, trung gian, trùng lặp và các biến missing cực đoan (>95%)
        cols_to_drop = [
            "title",
            "description",
            "title_norm",
            "desc_norm",
            "raw_cpu",
            "raw_ram",
            "raw_hard_drive",
            "raw_hard_drive_type",
            "pc_brand",
            "pc_cpu",
            "pc_ram",
            "laptop_screen_size",
            "elt_condition",
            "elt_origin",
            "elt_warranty",
        ]
        df_silver = df_features.drop(*cols_to_drop)

        logger.info(f"Writing data to Silver Parquet: {silver_dir}")
        os.makedirs(silver_dir, exist_ok=True)
        df_silver.write.format("parquet") \
            .option("compression", "snappy") \
            .mode("overwrite") \
            .save(silver_dir)

        logger.info("Successfully finished Silver layer ETL pipeline.")
        return df_silver

    except Exception as e:
        logger.error(f"Error in Silver layer ETL: {e}")
        raise
    finally:
        if is_owner:
            logger.info("Stopping SparkSession...")
            spark.stop()


if __name__ == "__main__":
    process_silver_layer()
