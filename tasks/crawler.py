import json
import re
import time
import logging
from abc import ABC, abstractmethod
from datetime import datetime

import boto3
import requests
from bs4 import BeautifulSoup

from core.config import settings

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
# 추상 베이스
# ══════════════════════════════════════════════════════════════
class BaseCrawler(ABC):
    brand_name: str
    total_pages: int = 1

    def crawl(self) -> list[dict]:
        results = []
        for page in range(1, self.total_pages + 1):
            logger.info(f"[{self.brand_name}] 페이지 {page}/{self.total_pages} 크롤링 중")
            items = self.crawl_page(page)
            results.extend(items)
            logger.info(f"  → {len(items)}개 수집 (누적 {len(results)}개)")
            time.sleep(0.5)
        return results

    @abstractmethod
    def crawl_page(self, page: int) -> list[dict]:
        ...

    @staticmethod
    def get(url: str, **kwargs) -> requests.Response:
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=15, **kwargs)
        return res

    @staticmethod
    def safe_float(val: str | None) -> float | None:
        try:
            return float(val.replace(",", "").strip()) if val else None
        except ValueError:
            return None

    @staticmethod
    def parse_volume_ml(text: str) -> int | None:
        m = re.search(r"(\d+)\s*ml", text, re.IGNORECASE)
        return int(m.group(1)) if m else None


# ══════════════════════════════════════════════════════════════
# 테라커피
# ══════════════════════════════════════════════════════════════
class TerraCoffeeCrawler(BaseCrawler):
    brand_name  = "teracoffee"
    total_pages = 6
    BASE_URL    = "https://teracoffee.com/default/drink/index.php"

    @staticmethod
    def _get_val(container, key: str) -> str | None:
        for font in container.find_all("font", style=lambda s: s and "12px" in s):
            text = font.get_text(strip=True)
            if key in text:
                return text.split(":")[-1].strip()
        return None

    def crawl_page(self, page: int) -> list[dict]:
        res = self.get(self.BASE_URL, params={"com_board_page": page})
        res.encoding = "euc-kr"
        soup = BeautifulSoup(res.text, "html.parser")

        items = []
        for title_tag in soup.select("span.gallery_title"):
            name = title_tag.get_text(strip=True)

            container = title_tag.parent
            while container:
                if container.find("font", style=lambda s: s and "12px" in s):
                    break
                container = container.parent

            if not container:
                continue

            volume_label = self._get_val(container, "용량")
            items.append({
                "brand":           self.brand_name,
                "name":            name,
                "category":        self._get_val(container, "카테고리"),
                "volume_label":    volume_label,
                "volume_ml":       self.parse_volume_ml(volume_label) if volume_label else None,
                "calories":        self.safe_float(self._get_val(container, "칼로리")),
                "carbs_g":         self.safe_float(self._get_val(container, "탄수화물")),
                "sugar_g":         self.safe_float(self._get_val(container, "당류")),
                "protein_g":       self.safe_float(self._get_val(container, "단백질")),
                "fat_g":           self.safe_float(self._get_val(container, "지방")),
                "saturated_fat_g": self.safe_float(self._get_val(container, "포화지방")),
                "sodium_mg":       self.safe_float(self._get_val(container, "나트륨")),
                "caffeine_mg":     self.safe_float(self._get_val(container, "카페인")),
                "allergens":       self._get_val(container, "알러지"),
                "crawled_at":      datetime.utcnow().isoformat(),
            })
        return items


# ══════════════════════════════════════════════════════════════
# 매머드커피
# ══════════════════════════════════════════════════════════════
class MammothCoffeeCrawler(BaseCrawler):
    brand_name  = "mammothcoffee"
    total_pages = 1  # 카테고리별 페이지네이션 없음
    LIST_URL    = "https://mmthcoffee.com/sub/menu/list_coffee_sub.php"
    DETAIL_URL  = "https://mmthcoffee.com/sub/menu/list_coffee_view.php"
    CATEGORIES  = ["C", "D", "N", "T", "B"]

    def crawl(self) -> list[dict]:
        results = []
        for cat in self.CATEGORIES:
            logger.info(f"[{self.brand_name}] 카테고리 {cat} 크롤링 중")
            items = self._crawl_category(cat)
            results.extend(items)
            logger.info(f"  → {len(items)}개 수집 (누적 {len(results)}개)")
        return results

    def crawl_page(self, page: int) -> list[dict]:
        # BaseCrawler 추상 메서드 충족용 — crawl() 오버라이드로 실제 사용 안 함
        return []

    def _crawl_category(self, menu_type: str) -> list[dict]:
        res = self.get(self.LIST_URL, params={"menuType": menu_type})
        ids = re.findall(r"goViewB\((\d+)\)", res.text)
        items = []
        for menu_seq in ids:
            records = self._crawl_detail(menu_seq)
            items.extend(records)
            time.sleep(0.3)
        return items

    def _crawl_detail(self, menu_seq: str) -> list[dict]:
        """HOT/ICE를 별도 레코드로 반환 (없으면 빈 리스트)."""
        res = self.get(self.DETAIL_URL, params={"menuSeq": menu_seq})
        soup = BeautifulSoup(res.text, "html.parser")

        # 음료명 — .i_tit 클래스
        name_tag = soup.select_one(".i_tit")
        if not name_tag:
            return []
        name = name_tag.get_text(strip=True)

        # 영양 정보 테이블: 첫 행이 헤더 ['구분', 'HOT(Xoz)', 'ICE(Xoz)']
        # 이후 행: ['칼로리 (Kcal)', hot_val, ice_val]
        tbl = soup.find("table")
        if not tbl:
            return []

        rows = tbl.find_all("tr")
        if not rows:
            return []

        # 헤더 파싱: HOT/ICE 열 인덱스 추출
        header_cells = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
        hot_idx = next((i for i, h in enumerate(header_cells) if "HOT" in h.upper()), None)
        ice_idx = next((i for i, h in enumerate(header_cells) if "ICE" in h.upper()), None)

        # 영양소 맵 구성
        nutrient_map: dict[str, list[str | None]] = {}  # key → [hot_val, ice_val]
        for row in rows[1:]:
            cells = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
            if len(cells) < 2:
                continue
            key = cells[0]
            hot_val = cells[hot_idx] if hot_idx and hot_idx < len(cells) else None
            ice_val = cells[ice_idx] if ice_idx and ice_idx < len(cells) else None
            nutrient_map[key] = [hot_val, ice_val]

        def _pick_val(idx, *keys):
            for k in keys:
                for nk, vals in nutrient_map.items():
                    if k in nk:
                        return vals[idx]
            return None

        records = []
        for idx, ice_type in [(0, "HOT"), (1, "ICE")]:
            records.append({
                "brand":           self.brand_name,
                "name":            name,
                "ice_type":        ice_type,
                "calories":        self.safe_float(_pick_val(idx, "칼로리")),
                "carbs_g":         self.safe_float(_pick_val(idx, "탄수화물")),
                "sugar_g":         self.safe_float(_pick_val(idx, "당류")),
                "protein_g":       self.safe_float(_pick_val(idx, "단백질")),
                "fat_g":           self.safe_float(_pick_val(idx, "지방")),
                "saturated_fat_g": self.safe_float(_pick_val(idx, "포화지방")),
                "sodium_mg":       self.safe_float(_pick_val(idx, "나트륨")),
                "caffeine_mg":     self.safe_float(_pick_val(idx, "카페인")),
                "crawled_at":      datetime.utcnow().isoformat(),
            })
        return records


# ══════════════════════════════════════════════════════════════
# 메가커피
# ══════════════════════════════════════════════════════════════
class MegaCoffeeCrawler(BaseCrawler):
    brand_name = "megacoffee"
    MENU_URL   = "https://www.mega-mgccoffee.com/menu/menu.php"

    def crawl(self) -> list[dict]:
        results = []
        page = 1
        while True:
            logger.info(f"[{self.brand_name}] 페이지 {page} 크롤링 중")
            items = self.crawl_page(page)
            if not items:
                break
            results.extend(items)
            logger.info(f"  → {len(items)}개 수집 (누적 {len(results)}개)")

            # 다음 페이지 존재 여부 확인
            res = self.get(self.MENU_URL, params={"page": page, "list_checkbox_all": "all"})
            soup = BeautifulSoup(res.text, "html.parser")
            page_links = [a.get("data-page", "") for a in soup.select("#board_page li a.board_page_link")]
            max_page = max((int(p) for p in page_links if p.isdigit()), default=page)
            if page >= max_page:
                break
            page += 1
            time.sleep(0.5)
        return results

    def crawl_page(self, page: int) -> list[dict]:
        res = self.get(self.MENU_URL, params={"page": page, "list_checkbox_all": "all"})
        soup = BeautifulSoup(res.text, "html.parser")

        items = []
        for li in soup.select("#menu_list > li"):
            name_tag = li.select_one(".cont_text_title b")
            if not name_tag:
                continue
            name = name_tag.get_text(strip=True)

            ice_label = li.select_one(".cont_gallery_list_label")
            ice_type = ice_label.get_text(strip=True) if ice_label else None

            modal = li.select_one(".inner_modal")
            if not modal:
                continue

            infos = [d.get_text(strip=True) for d in modal.select(".cont_text .cont_text_inner")]
            volume_text  = next((t for t in infos if "ml" in t), None)
            calorie_text = next((t for t in infos if "kcal" in t), None)

            nutrients = {
                n.split()[0]: self.safe_float(re.sub(r"[^\d.]", "", n.split()[-1]))
                for n in (li.get_text(strip=True) for li in modal.select(".cont_list ul li"))
                if len(n.split()) >= 2
            }

            items.append({
                "brand":           self.brand_name,
                "name":            name,
                "ice_type":        ice_type,
                "volume_ml":       self.parse_volume_ml(volume_text) if volume_text else None,
                "calories":        self.safe_float(re.sub(r"[^\d.]", "", calorie_text.replace("1회 제공량", ""))) if calorie_text else None,
                "saturated_fat_g": nutrients.get("포화지방"),
                "sugar_g":         nutrients.get("당류"),
                "sodium_mg":       nutrients.get("나트륨"),
                "protein_g":       nutrients.get("단백질"),
                "caffeine_mg":     nutrients.get("카페인"),
                "crawled_at":      datetime.utcnow().isoformat(),
            })
        return items


# ══════════════════════════════════════════════════════════════
# 컴포즈커피
# ══════════════════════════════════════════════════════════════
class ComposeCoffeeCrawler(BaseCrawler):
    brand_name  = "composecoffee"
    BASE_URL    = "https://composecoffee.com/menu/category"
    # MD상품·콤보 제외, 음료 카테고리만
    CATEGORIES  = [185, 187, 192, 193, 188, 191, 339]

    def crawl(self) -> list[dict]:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        results = []
        for cat_id in self.CATEGORIES:
            page = 1
            while True:
                logger.info(f"[{self.brand_name}] category {cat_id} page {page}")
                items = self._crawl_category_page(cat_id, page)
                if not items:
                    break
                results.extend(items)
                logger.info(f"  → {len(items)}개 수집 (누적 {len(results)}개)")

                # 다음 페이지 존재 여부
                res = self.get(f"{self.BASE_URL}/{cat_id}", params={"page": page}, verify=False)
                soup = BeautifulSoup(res.text, "html.parser")
                next_link = soup.select_one('a[aria-label="Next"]')
                if not next_link or not next_link.get("href"):
                    break
                page += 1
                time.sleep(0.5)
        return results

    def crawl_page(self, page: int) -> list[dict]:
        return []  # BaseCrawler 추상 메서드 충족용

    def _crawl_category_page(self, cat_id: int, page: int) -> list[dict]:
        res = self.get(f"{self.BASE_URL}/{cat_id}", params={"page": page}, verify=False)
        soup = BeautifulSoup(res.text, "html.parser")

        items = []
        for box in soup.select(".itemBox"):
            name_tag = box.select_one("h4.title")
            if not name_tag:
                continue

            full_name = name_tag.get_text(separator=" ", strip=True)
            # 제목에서 HOT/ICE 추출 후 음료명 정리
            if full_name.upper().startswith("HOT"):
                ice_type = "HOT"
                drink_name = full_name[3:].strip()
            elif full_name.upper().startswith("ICE"):
                ice_type = "ICE"
                drink_name = full_name[3:].strip()
            else:
                ice_type = None
                drink_name = full_name

            extras = [li.get_text(" ", strip=True) for li in box.select("li.extra") if li.get_text(strip=True)]

            def _extract(keyword: str) -> str | None:
                for e in extras:
                    if keyword in e:
                        # "⚬ 열량(kcal) : 15" 또는 "⚬ 카페인 - 2shot : 156mg/45ml"
                        # 마지막 숫자 그룹 추출
                        nums = re.findall(r"[\d.]+", e.split(":")[-1])
                        return nums[0] if nums else None
                return None

            items.append({
                "brand":           self.brand_name,
                "name":            drink_name,
                "ice_type":        ice_type,
                "volume_ml":       self.parse_volume_ml(next((e for e in extras if "용량" in e), "") or ""),
                "calories":        self.safe_float(_extract("열량")),
                "sodium_mg":       self.safe_float(_extract("나트륨")),
                "carbs_g":         self.safe_float(_extract("탄수화물")),
                "sugar_g":         self.safe_float(_extract("당류")),
                "fat_g":           self.safe_float(_extract("지방") if "포화" not in (_extract("지방") or "") else None),
                "saturated_fat_g": self.safe_float(_extract("포화지방")),
                "protein_g":       self.safe_float(_extract("단백질")),
                "caffeine_mg":     self.safe_float(_extract("카페인")),
                "crawled_at":      datetime.utcnow().isoformat(),
            })
        return items


# ══════════════════════════════════════════════════════════════
# 스타벅스
# ══════════════════════════════════════════════════════════════
class StarbucksCrawler(BaseCrawler):
    brand_name = "starbucks"
    BASE_URL   = "https://www.starbucks.co.kr/upload/json/menu"
    CATEGORIES = [
        "W0000171",  # 콜드 브루
        "W0000060",  # 브루드 커피
        "W0000003",  # 에스프레소
        "W0000004",  # 프라푸치노
        "W0000005",  # 블렌디드
        "W0000422",  # 리프레셔
        "W0000061",  # 피지오
        "W0000075",  # 티 (티바나)
        "W0000053",  # 기타 제조 음료
        "W0000062",  # 주스
    ]

    def crawl(self) -> list[dict]:
        results = []
        for cat_cd in self.CATEGORIES:
            logger.info(f"[{self.brand_name}] 카테고리 {cat_cd} 크롤링 중")
            items = self._crawl_category(cat_cd)
            results.extend(items)
            logger.info(f"  → {len(items)}개 수집 (누적 {len(results)}개)")
            time.sleep(0.5)
        return results

    def crawl_page(self, page: int) -> list[dict]:
        return []  # BaseCrawler 추상 메서드 충족용

    def _crawl_category(self, cat_cd: str) -> list[dict]:
        res = self.get(f"{self.BASE_URL}/{cat_cd}.js")
        try:
            data = res.json()
        except Exception:
            m = re.search(r"\{.*\}", res.text, re.DOTALL)
            if not m:
                return []
            data = json.loads(m.group(0))

        items = []
        for product in data.get("list", []):
            if product.get("sold_OUT") == "Y":
                continue

            name = (product.get("product_NM") or "").strip()
            if not name:
                continue

            def _f(val) -> float | None:
                try:
                    return float(val) if val is not None else None
                except (ValueError, TypeError):
                    return None

            items.append({
                "brand":           self.brand_name,
                "name":            name,
                "caffeine_mg":     _f(product.get("caffeine")),
                "calories":        _f(product.get("kcal")),
                "carbs_g":         _f(product.get("chabo")),
                "sugar_g":         _f(product.get("sugars")),
                "protein_g":       _f(product.get("protein")),
                "fat_g":           _f(product.get("fat")),
                "sodium_mg":       _f(product.get("sodium")),
                "crawled_at":      datetime.utcnow().isoformat(),
            })
        return items


# ══════════════════════════════════════════════════════════════
# 빽다방
# ══════════════════════════════════════════════════════════════
class PaikdabangCrawler(BaseCrawler):
    brand_name = "paikdabang"
    BASE_URL   = "https://paikdabang.com/menu"
    CATEGORIES = ["menu_coffee", "menu_drink", "menu_dessert", "menu_ccino"]

    def crawl(self) -> list[dict]:
        results = []
        for cat in self.CATEGORIES:
            logger.info(f"[{self.brand_name}] 카테고리 {cat} 크롤링 중")
            items = self._crawl_category(cat)
            results.extend(items)
            logger.info(f"  → {len(items)}개 수집 (누적 {len(results)}개)")
            time.sleep(0.5)
        return results

    def crawl_page(self, page: int) -> list[dict]:
        return []  # BaseCrawler 추상 메서드 충족용

    def _crawl_category(self, cat: str) -> list[dict]:
        res = self.get(f"{self.BASE_URL}/{cat}/")
        soup = BeautifulSoup(res.text, "html.parser")

        items = []
        for hover in soup.select("div.hover"):
            name_tag = hover.select_one("h3")
            if not name_tag:
                continue
            name = name_tag.get_text(strip=True)

            nutrients = {}
            for li in hover.select("ul.ingredient_table > li"):
                divs = li.select("div")
                if len(divs) >= 2:
                    nutrients[divs[0].get_text(strip=True)] = divs[1].get_text(strip=True)

            volume_ml = None
            for p in hover.select("p"):
                text = p.get_text(strip=True)
                if "1회 제공량" in text:
                    volume_ml = self.parse_volume_ml(text)
                    break

            def _get(key_part: str) -> float | None:
                for k, v in nutrients.items():
                    if key_part in k:
                        return self.safe_float(v)
                return None

            items.append({
                "brand":           self.brand_name,
                "name":            name,
                "caffeine_mg":     _get("카페인"),
                "calories":        _get("칼로리"),
                "sodium_mg":       _get("나트륨"),
                "sugar_g":         _get("당류"),
                "saturated_fat_g": _get("포화지방"),
                "protein_g":       _get("단백질"),
                "volume_ml":       volume_ml,
                "crawled_at":      datetime.utcnow().isoformat(),
            })
        return items


# ══════════════════════════════════════════════════════════════
# 하삼동커피
# ══════════════════════════════════════════════════════════════
class HasamdongCrawler(BaseCrawler):
    brand_name   = "hasamdong"
    BASE_URL     = "https://www.hasamdongcoffee.com"
    LIST_AJAX    = "https://www.hasamdongcoffee.com/menu_list_ajax.php"
    DETAIL_AJAX  = "https://www.hasamdongcoffee.com/menu_dtl_pop_ajax.php"
    # 음료 카테고리만 수집 (30=보틀, 90/100/110=굿즈 등 제외)
    CATEGORIES   = ["20", "40", "50", "60", "70", "80"]

    def _make_session(self) -> requests.Session:
        from urllib3.util.ssl_ import create_urllib3_context
        from requests.adapters import HTTPAdapter

        class _LegacySSL(HTTPAdapter):
            def init_poolmanager(self, *args, **kwargs):
                ctx = create_urllib3_context()
                ctx.set_ciphers("DEFAULT@SECLEVEL=1")
                kwargs["ssl_context"] = ctx
                super().init_poolmanager(*args, **kwargs)

        s = requests.Session()
        s.mount("https://", _LegacySSL())
        s.headers.update({"User-Agent": "Mozilla/5.0",
                          "Referer": f"{self.BASE_URL}/menu_list.php"})
        return s

    def crawl(self) -> list[dict]:
        session = self._make_session()
        results = []
        for cat_cd in self.CATEGORIES:
            logger.info(f"[{self.brand_name}] 카테고리 {cat_cd} 크롤링 중")
            items = self._crawl_category(session, cat_cd)
            results.extend(items)
            logger.info(f"  → {len(items)}개 수집 (누적 {len(results)}개)")
            time.sleep(0.5)
        return results

    def crawl_page(self, page: int) -> list[dict]:
        return []  # BaseCrawler 추상 메서드 충족용

    def _crawl_category(self, session: requests.Session, cat_cd: str) -> list[dict]:
        res = session.get(self.LIST_AJAX, params={"sc_pd_ctg_cd": cat_cd}, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")

        items = []
        for a in soup.select("a.btnView[seq]"):
            seq  = a.get("seq")
            name = a.select_one("div.detail p")
            if not seq or not name:
                continue
            records = self._crawl_detail(session, seq, name.get_text(strip=True))
            items.extend(records)
            time.sleep(0.3)
        return items

    def _crawl_detail(self, session: requests.Session, seq: str, name: str) -> list[dict]:
        res = session.post(self.DETAIL_AJAX,
                           data={"pk_seq": seq},
                           headers={"X-Requested-With": "XMLHttpRequest"},
                           timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")

        # thead 순서: 구분 | 1회제공량 | 열량 | 단백질 | 당류 | 포화지방 | 나트륨 | 카페인 | 알레르기
        headers = [th.get_text(strip=True) for th in soup.select("thead th")]

        # 열 순서: 구분|1회제공량|열량|단백질|당류|포화지방|나트륨|카페인|알레르기
        COL = {"ice": 0, "kcal": 2, "protein": 3, "sugar": 4,
               "sat_fat": 5, "sodium": 6, "caffeine": 7}

        records = []
        for tr in soup.select("tbody tr.tr1, tbody tr.tr2"):
            cells = [td.get_text(strip=True) for td in tr.select("td")]
            if len(cells) < 8 or not cells[COL["ice"]]:
                continue

            records.append({
                "brand":           self.brand_name,
                "name":            name,
                "ice_type":        cells[COL["ice"]].upper(),
                "caffeine_mg":     self.safe_float(cells[COL["caffeine"]]),
                "calories":        self.safe_float(cells[COL["kcal"]]),
                "protein_g":       self.safe_float(cells[COL["protein"]]),
                "sugar_g":         self.safe_float(cells[COL["sugar"]]),
                "saturated_fat_g": self.safe_float(cells[COL["sat_fat"]]),
                "sodium_mg":       self.safe_float(cells[COL["sodium"]]),
                "crawled_at":      datetime.utcnow().isoformat(),
            })
        return records


# ══════════════════════════════════════════════════════════════
# 이디야
# ══════════════════════════════════════════════════════════════
class EdiyaCrawler(BaseCrawler):
    brand_name   = "ediya"
    BASE_URL     = "https://ediya.com/contents/drink.html"
    AJAX_URL     = "https://ediya.com/inc/ajax_brand.php"
    PRODUCT_CATE = "7"

    def crawl(self) -> list[dict]:
        results = []
        page = 1
        while True:
            res = self.get(self.AJAX_URL, params={
                "product_cate": self.PRODUCT_CATE,
                "gubun": "menu_more",
                "chked_val": "",
                "skeyword": "",
                "page": page,
            })
            soup = BeautifulSoup(res.text, "html.parser")
            lis = [li for li in soup.find_all("li") if li.find(class_="pro_detail")]
            if not lis:
                break
            items, _ = self._parse_lis(lis)
            results.extend(items)
            page += 1
            time.sleep(0.3)

        # (name, ice_type) 중복 시 카페인 높은 쪽(일반 버전) 우선 유지
        best: dict[tuple, dict] = {}
        for item in results:
            key = (item["name"], item["ice_type"])
            if key not in best or (item["caffeine_mg"] or 0) > (best[key]["caffeine_mg"] or 0):
                best[key] = item
        return list(best.values())

    def crawl_page(self, page: int) -> list[dict]:
        return []

    def _parse_lis(self, lis) -> tuple[list[dict], list[str]]:
        items, ids = [], []
        for li in lis:
            detail = li.select_one("div.pro_detail")
            if not detail:
                continue
            nutri_id = detail.get("id", "")  # e.g. "nutri_1186"

            name_tag = li.select_one("div.menu_tt a span")
            if not name_tag:
                continue
            full_name = name_tag.get_text(strip=True)

            # "(L) ICED 거문도쑥 라떼" → size=(L), ice_type, 음료명
            m = re.match(r"^\(([^)]+)\)\s+(HOT|ICED|ICE)\s+(.+)$", full_name, re.IGNORECASE)
            if m:
                size      = m.group(1).strip()        # "L", "EX", "M" 등
                ice_type  = "hot" if m.group(2).upper() == "HOT" else "ice"
                drink_name = f"{m.group(3).strip()}({size})"  # "거문도쑥 라떼(L)"
            else:
                drink_name = full_name
                name_lower = full_name.lower()
                if any(k in name_lower for k in ["iced", "아이스", "cold"]):
                    ice_type = "ice"
                elif any(k in name_lower for k in ["hot", "핫", "따뜻"]):
                    ice_type = "hot"
                else:
                    ice_type = None

            # 영양정보: dd 값 형식 "(359kcal)" → 숫자만 추출
            def _num(text: str) -> float | None:
                nums = re.findall(r"[\d.]+", text)
                return float(nums[0]) if nums else None

            nutrients: dict[str, str] = {}
            nutri_div = li.select_one("div.pro_nutri")
            if nutri_div:
                for dl in nutri_div.find_all("dl"):
                    dt = dl.find("dt")
                    dd = dl.find("dd")
                    if dt and dd:
                        nutrients[dt.get_text(strip=True)] = dd.get_text(strip=True)

            size_div = li.select_one("div.pro_size")

            items.append({
                "brand":           self.brand_name,
                "name":            drink_name,
                "ice_type":        ice_type,
                "volume_ml":       self.parse_volume_ml(size_div.get_text()) if size_div else None,
                "calories":        _num(nutrients.get("칼로리", "")),
                "sugar_g":         _num(nutrients.get("당류", "") or nutrients.get(" 당류", "")),
                "protein_g":       _num(nutrients.get("단백질", "") or nutrients.get(" 단백질", "")),
                "saturated_fat_g": _num(nutrients.get("포화지방", "") or nutrients.get(" 포화지방", "")),
                "sodium_mg":       _num(nutrients.get("나트륨", "") or nutrients.get(" 나트륨", "")),
                "caffeine_mg":     _num(nutrients.get("카페인", "") or nutrients.get(" 카페인", "")),
                "crawled_at":      datetime.utcnow().isoformat(),
            })
            ids.append(nutri_id)
        return items, ids


# ══════════════════════════════════════════════════════════════
# 커피빈
# ══════════════════════════════════════════════════════════════
class CoffeeBeanCrawler(BaseCrawler):
    brand_name = "coffeebean"
    BASE_URL   = "https://www.coffeebeankorea.com/menu/list.asp"
    # 음료 카테고리만 (푸드/상품 제외)
    CATEGORIES = [32, 13, 14, 18, 17, 12, 11, 26, 24]

    def crawl(self) -> list[dict]:
        results = []
        for cat in self.CATEGORIES:
            logger.info(f"[{self.brand_name}] 카테고리 {cat} 크롤링 중")
            items = self._crawl_category(cat)
            results.extend(items)
            logger.info(f"  → {len(items)}개 수집 (누적 {len(results)}개)")
            time.sleep(0.5)
        return results

    def crawl_page(self, page: int) -> list[dict]:
        return []

    def _crawl_category(self, category: int) -> list[dict]:
        res = self.get(self.BASE_URL, params={"category": category})
        soup = BeautifulSoup(res.text, "html.parser")

        items = []
        for li in soup.select("ul.menu_list li"):
            kor_tag = li.select_one("span.kor")
            eng_tag = li.select_one("span.eng")
            if not kor_tag:
                continue

            name     = kor_tag.get_text(strip=True)
            eng_name = eng_tag.get_text(strip=True).lower() if eng_tag else ""

            if eng_name.startswith("iced") or " iced " in eng_name:
                ice_type = "ice"
            elif eng_name.startswith("hot") or " hot " in eng_name:
                ice_type = "hot"
            else:
                ice_type = None

            # div.info > dl : dt=값, dd=영양소명+단위
            nutrients: dict[str, float | None] = {}
            for dl in li.select("div.info dl"):
                dt = dl.find("dt")
                dd = dl.find("dd")
                if dt and dd:
                    key  = dd.get_text(strip=True)
                    nums = re.findall(r"[\d.]+", dt.get_text(strip=True))
                    nutrients[key] = float(nums[0]) if nums else None

            def _get(keyword: str) -> float | None:
                for k, v in nutrients.items():
                    if keyword in k:
                        return v
                return None

            items.append({
                "brand":           self.brand_name,
                "name":            name,
                "ice_type":        ice_type,
                "calories":        _get("열량"),
                "sodium_mg":       _get("나트륨"),
                "carbs_g":         _get("탄수화물"),
                "sugar_g":         _get("당"),
                "protein_g":       _get("단백질"),
                "caffeine_mg":     _get("카페인"),
                "saturated_fat_g": _get("포화지방"),
                "crawled_at":      datetime.utcnow().isoformat(),
            })
        return items


# ══════════════════════════════════════════════════════════════
# 할리스
# ══════════════════════════════════════════════════════════════
class HollysCrawler(BaseCrawler):
    brand_name  = "hollys"
    BASE_URL    = "https://www.hollys.co.kr/menu"
    # 음료 카테고리만 (푸드/MD 제외)
    CATEGORIES  = ["espresso", "signature", "hollyccino", "juice", "tea"]
    _HEADERS    = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer":    "https://www.hollys.co.kr/",
    }

    def crawl(self) -> list[dict]:
        results = []
        for cat in self.CATEGORIES:
            logger.info(f"[{self.brand_name}] 카테고리 {cat} 크롤링 중")
            items = self._crawl_category(cat)
            results.extend(items)
            logger.info(f"  → {len(items)}개 수집 (누적 {len(results)}개)")
            time.sleep(0.5)
        return results

    def crawl_page(self, page: int) -> list[dict]:
        return []

    def _crawl_category(self, cat: str) -> list[dict]:
        res = requests.get(f"{self.BASE_URL}/{cat}.do", headers=self._HEADERS, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")

        items = []
        for view1 in soup.select("div.menu_view01"):
            item_id = view1.get("id", "").replace("menuView1_", "")
            if not item_id:
                continue

            name_tag = view1.select_one("div.menu_detail p span")
            if not name_tag:
                continue
            name = name_tag.get_text(strip=True)

            view2 = soup.find(id=f"menuView2_{item_id}")
            if not view2:
                continue

            table = view2.find("table")
            if not table:
                continue

            headers = [th.get_text(strip=True) for th in table.select("thead th")]

            for tr in table.select("tbody tr"):
                cells = [c.get_text(strip=True) for c in tr.find_all(["th", "td"])]
                if len(cells) < 2:
                    continue

                ice_raw = cells[0].upper()
                if "ICE" in ice_raw:
                    ice_type = "ice"
                elif "HOT" in ice_raw:
                    ice_type = "hot"
                else:
                    ice_type = None

                def _col(keyword: str) -> float | None:
                    for i, h in enumerate(headers):
                        if keyword in h and i < len(cells):
                            nums = re.findall(r"[\d.]+", cells[i])
                            return float(nums[0]) if nums else None
                    return None

                items.append({
                    "brand":           self.brand_name,
                    "name":            name,
                    "ice_type":        ice_type,
                    "calories":        _col("칼로리"),
                    "sugar_g":         _col("당류"),
                    "protein_g":       _col("단백질"),
                    "saturated_fat_g": _col("포화지방"),
                    "sodium_mg":       _col("나트륨"),
                    "caffeine_mg":     _col("카페인"),
                    "crawled_at":      datetime.utcnow().isoformat(),
                })
        return items


# ══════════════════════════════════════════════════════════════
# 브랜드 레지스트리 — 새 브랜드는 여기에만 추가
# ══════════════════════════════════════════════════════════════
BRAND_REGISTRY: dict[str, type[BaseCrawler]] = {
    "teracoffee":    TerraCoffeeCrawler,
    "mammothcoffee": MammothCoffeeCrawler,
    "megacoffee":    MegaCoffeeCrawler,
    "composecoffee": ComposeCoffeeCrawler,
    "starbucks":     StarbucksCrawler,
    "paikdabang":    PaikdabangCrawler,
    "hasamdong":     HasamdongCrawler,
    "ediya":         EdiyaCrawler,
    "coffeebean":    CoffeeBeanCrawler,
    "hollys":        HollysCrawler,
}


# ══════════════════════════════════════════════════════════════
# S3 업로드
# ══════════════════════════════════════════════════════════════
def _get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=settings.AWS_REGION,
    )


def upload_raw_to_s3(brand: str, data: list[dict]) -> str:
    date_str = datetime.utcnow().strftime("%Y%m%d")
    s3_key = f"raw/brand={brand}/dt={date_str}/drinks.json"

    body = "\n".join(json.dumps(row, ensure_ascii=False) for row in data).encode("utf-8")
    _get_s3_client().put_object(
        Bucket=settings.AWS_S3_BUCKET_NAME,
        Key=s3_key,
        Body=body,
        ContentType="application/json",
    )
    logger.info(f">>> [Crawler] S3 업로드 완료: s3://{settings.AWS_S3_BUCKET_NAME}/{s3_key}")
    return s3_key


async def run_crawler(brand: str = "teracoffee") -> str:
    """크롤링 실행 → S3 raw 저장. 저장된 S3 key 반환."""
    crawler_cls = BRAND_REGISTRY.get(brand)
    if not crawler_cls:
        raise ValueError(f"등록되지 않은 브랜드: {brand}")

    logger.info(f">>> [Crawler] {brand} 크롤링 시작...")
    data = crawler_cls().crawl()
    logger.info(f">>> [Crawler] {len(data)}개 수집 완료")

    return upload_raw_to_s3(brand, data)
