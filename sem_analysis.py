import pandas as pd
import yaml
import numpy as np
from datetime import datetime
import csv
import os
from dotenv import load_dotenv
load_dotenv()
import re
import time

def _extract_json_object(text: str):
    import json
    if not text:
        raise ValueError("Empty LLM response")
    fence = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", text, re.IGNORECASE)
    if fence:
        return json.loads(fence.group(1))
    fence_any = re.search(r"```[a-zA-Z]*\s*(\{[\s\S]*?\})\s*```", text)
    if fence_any:
        return json.loads(fence_any.group(1))
    start = text.find('{')
    end = text.rfind('}')
    if start != -1 and end != -1 and end > start:
        candidate = text[start:end+1]
        return json.loads(candidate)
    raise ValueError("No JSON object found in LLM response")

class SEMAnalysis:
    def __init__(self, keywords_file: str, config_file: str = "config.yaml", gemini_api_key: str = None):
        self.keywords_data = self.load_keywords(keywords_file)
        self.config = self.load_config(config_file)
        self.analysis_results = {}
        if gemini_api_key:
            try:
                import google.generativeai as genai
                genai.configure(api_key=gemini_api_key)
                self.model = genai.GenerativeModel('gemini-1.5-flash')
                self.use_llm = True
            except ImportError:
                self.use_llm = False
            except Exception:
                try:
                    self.model = genai.GenerativeModel('gemini-1.5-pro')
                    self.use_llm = True
                except:
                    try:
                        self.model = genai.GenerativeModel('gemini-pro')
                        self.use_llm = True
                    except:
                        self.use_llm = False
        else:
            self.use_llm = False

    def _call_llm_json(self, prompt: str, retries: int = 2):
        last_err = None
        for attempt in range(retries + 1):
            try:
                response = self.model.generate_content(prompt)
                return _extract_json_object(response.text)
            except Exception as e:
                last_err = e
                msg = str(e)
                if '429' in msg or 'quota' in msg.lower():
                    time.sleep(10 * (attempt + 1))
                prompt = prompt + "\n\nReturn ONLY valid minified JSON with no code fences and no extra text."
        raise last_err

    def _call_llm_json_array(self, prompt: str, retries: int = 2):
        last_err = None
        for attempt in range(retries + 1):
            try:
                response = self.model.generate_content(prompt)
                text = response.text or ""
                arr = re.search(r"```json\s*(\[[\s\S]*?\])\s*```", text, re.IGNORECASE)
                if arr:
                    import json
                    return json.loads(arr.group(1))
                start = text.find('[')
                end = text.rfind(']')
                if start != -1 and end != -1 and end > start:
                    import json
                    return json.loads(text[start:end+1])
                raise ValueError("No JSON array found in LLM response")
            except Exception as e:
                last_err = e
                msg = str(e)
                if '429' in msg or 'quota' in msg.lower():
                    time.sleep(10 * (attempt + 1))
                prompt = prompt + "\n\nReturn ONLY valid minified JSON array with no code fences and no extra text."
        raise last_err

    def _batch(self, items, size):
        for i in range(0, len(items), size):
            yield items[i:i+size]

    def load_keywords(self, keywords_file: str):
        try:
            df = pd.read_csv(keywords_file)
            return df
        except FileNotFoundError:
            raise FileNotFoundError(f"Keywords file {keywords_file} not found")

    def load_config(self, config_file: str):
        try:
            with open(config_file, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file {config_file} not found")

    def analyze_performance_indicators(self):
        self.keywords_data['avg_bid'] = (self.keywords_data['top_of_page_bid_low'] + self.keywords_data['top_of_page_bid_high']) / 2
        search_volume_stats = self.keywords_data['search_volume'].describe()
        bid_stats = self.keywords_data['avg_bid'].describe()
        high_volume_threshold = self.keywords_data['search_volume'].quantile(0.8)
        high_volume_keywords = self.keywords_data[self.keywords_data['search_volume'] >= high_volume_threshold]
        cost_effective = self.keywords_data[(self.keywords_data['avg_bid'] <= bid_stats['50%']) & (self.keywords_data['search_volume'] >= search_volume_stats['50%'])]
        low_comp_high_vol = self.keywords_data[(self.keywords_data['competition'].isin(['Low', 'Medium'])) & (self.keywords_data['search_volume'] >= search_volume_stats['50%'])]
        return {
            'high_volume_keywords': high_volume_keywords,
            'cost_effective_keywords': cost_effective,
            'low_comp_high_vol': low_comp_high_vol
        }

    def analyze_keyword_intent_with_llm(self, keyword: str, search_volume: int, competition: str):
        if not self.use_llm:
            raise RuntimeError("LLM is required for keyword intent analysis. Set GEMINI_API_KEY in .env.")
        brand_name = self.extract_brand_name(self.config.get('brand_website', ''))
        competitor_name = self.extract_brand_name(self.config.get('competitor_website', ''))
        prompt = f"""
You are an SEM expert. Classify the keyword for campaign structuring.
Keyword: "{keyword}"
Search Volume: {search_volume}
Competition: {competition}
Brand: {brand_name}
Competitor: {competitor_name}
Return STRICT JSON only with keys:
{{
  "ad_group": "<group>",
  "intent": "<intent>",
  "match_type": "<match>",
  "reasoning": "<brief>"
}}
"""
        return self._call_llm_json(prompt)

    def create_ad_groups_with_llm(self):
        from collections import defaultdict
        ad_groups = defaultdict(list)
        rows = []
        for idx, row in self.keywords_data.iterrows():
            rows.append({
                'id': int(idx),
                'keyword': row['keyword'],
                'search_volume': int(row['search_volume']),
                'competition': str(row['competition'])
            })
        brand_name = self.extract_brand_name(self.config.get('brand_website', ''))
        competitor_name = self.extract_brand_name(self.config.get('competitor_website', ''))
        for chunk in self._batch(rows, 15):
            prompt = f"""
You are an SEM expert. Classify each keyword record for campaign structuring.
Brand: {brand_name}
Competitor: {competitor_name}
Records:
{chunk}
Return ONLY a JSON array of objects:
[
  {{"id": <id>, "ad_group": "<group>", "intent": "<intent>", "match_type": "<match>", "reasoning": "<brief>"}}
]
"""
            results = self._call_llm_json_array(prompt)
            result_by_id = {int(item.get('id')): item for item in results if isinstance(item, dict) and 'id' in item}
            for item_id, item in result_by_id.items():
                df_row = self.keywords_data.loc[item_id]
                row_copy = df_row.copy()
                row_copy['llm_ad_group'] = item.get('ad_group')
                row_copy['llm_intent'] = item.get('intent')
                row_copy['llm_match_type'] = item.get('match_type')
                row_copy['llm_reasoning'] = item.get('reasoning')
                group = row_copy['llm_ad_group'] or 'Uncategorized'
                ad_groups[group].append(row_copy)
        return dict(ad_groups)

    def suggest_match_types(self, keyword):
        keyword_text = keyword['keyword']
        word_count = len(keyword_text.split())
        if word_count == 1:
            return "Broad Match Modifier"
        elif word_count == 2:
            return "Phrase Match"
        return "Exact Match"

    def calculate_target_cpc(self):
        assumptions = self.config.get('assumptions') or {}
        missing = [k for k in ['ctr', 'conversion_rate'] if k not in assumptions]
        if missing:
            raise RuntimeError(f"Missing assumptions in config: {', '.join(missing)}")
        total_budget = float(self.config.get('search_ads_budget', 0))
        total_volume = float(self.keywords_data['search_volume'].sum())
        ctr = float(assumptions['ctr'])
        conversion_rate = float(assumptions['conversion_rate'])
        avg_bid_series = (self.keywords_data['top_of_page_bid_low'] + self.keywords_data['top_of_page_bid_high']) / 2
        cap_cpc = float(assumptions.get('max_cpc_cap', avg_bid_series.median()))
        expected_clicks = total_volume * ctr
        expected_conversions = expected_clicks * conversion_rate
        if expected_conversions <= 0 or total_budget <= 0:
            raise RuntimeError("Insufficient data to compute CPC")
        target_cpa = total_budget / expected_conversions
        target_cpc = target_cpa * conversion_rate
        return min(target_cpc, cap_cpc)

    def create_search_campaign_keywords(self):
        ad_groups = self.create_ad_groups_with_llm()
        target_cpc = self.calculate_target_cpc()
        search_campaign = {}
        for group_name, keywords in ad_groups.items():
            if not keywords:
                continue
            group_data = []
            for keyword in keywords:
                match_type = keyword.get('llm_match_type') if isinstance(keyword, dict) else ''
                avg_bid = (keyword['top_of_page_bid_low'] + keyword['top_of_page_bid_high']) / 2
                if keyword['competition'] == 'High':
                    suggested_cpc = min(avg_bid * 1.2, target_cpc * 1.5)
                elif keyword['competition'] == 'Medium':
                    suggested_cpc = min(avg_bid * 1.0, target_cpc)
                else:
                    suggested_cpc = min(avg_bid * 0.8, target_cpc * 0.8)
                group_data.append({
                    'keyword': keyword['keyword'],
                    'search_volume': keyword['search_volume'],
                    'match_type': match_type,
                    'suggested_cpc': round(suggested_cpc, 2),
                    'competition': keyword['competition'],
                    'source': keyword['source'],
                    'ad_group': keyword.get('llm_ad_group', group_name),
                    'intent': keyword.get('llm_intent', ''),
                    'reasoning': keyword.get('llm_reasoning', '')
                })
            search_campaign[group_name] = group_data
        return search_campaign

    def create_pmax_themes(self):
        brand_website = self.config.get('brand_website', '')
        competitor_website = self.config.get('competitor_website', '')
        brand_name = self.extract_brand_name(brand_website)
        competitor_name = self.extract_brand_name(competitor_website)
        parsed = self.generate_pmax_themes_with_llm(brand_name, competitor_name)
        return parsed

    def generate_pmax_themes_with_llm(self, brand_name, competitor_name):
        top_keywords = self.keywords_data.head(20)
        keyword_summary = []
        for _, row in top_keywords.iterrows():
            keyword_summary.append({
                'keyword': row['keyword'],
                'search_volume': row['search_volume'],
                'competition': row['competition']
            })
        prompt = f"""
Analyze these keywords for {brand_name} (competitor: {competitor_name}) and generate Performance Max themes.
Top Keywords:
{keyword_summary}
Return JSON:
{{
  "Product Category Themes": ["t1","t2","t3","t4","t5"],
  "Use-case Based Themes": ["t1","t2","t3","t4","t5"],
  "Demographic Themes": ["t1","t2","t3","t4","t5"],
  "Seasonal/Event-Based Themes": ["t1","t2","t3","t4","t5"]
}}
"""
        parsed = self._call_llm_json(prompt)
        return parsed

    def extract_brand_name(self, url):
        if not url:
            return "Unknown"
        url = url.replace('https://', '').replace('http://', '').replace('www.', '')
        domain = url.split('/')[0]
        if '.' in domain:
            brand = domain.split('.')[0]
            return brand.title()
        return domain.title()

    def calculate_shopping_cpc_bids(self):
        shopping_budget = self.config['shopping_ads_budget']
        conversion_rate = float(self.config.get('assumptions', {}).get('conversion_rate', 0.02))
        product_keywords = self.keywords_data
        if len(product_keywords) == 0:
            product_keywords = self.keywords_data.head(10)
        shopping_bids = []
        for _, keyword in product_keywords.iterrows():
            avg_bid = (keyword['top_of_page_bid_low'] + keyword['top_of_page_bid_high']) / 2
            budget_per_keyword = shopping_budget / max(len(product_keywords), 1)
            expected_clicks = keyword['search_volume'] * 0.01
            expected_conversions = expected_clicks * conversion_rate
            if expected_conversions > 0:
                target_cpa = budget_per_keyword / expected_conversions
                target_cpc = target_cpa * conversion_rate
            else:
                target_cpc = avg_bid
            if keyword['competition'] == 'High':
                suggested_cpc = min(target_cpc * 1.3, avg_bid * 1.5)
            elif keyword['competition'] == 'Medium':
                suggested_cpc = min(target_cpc * 1.1, avg_bid * 1.2)
            else:
                suggested_cpc = min(target_cpc * 0.9, avg_bid * 0.8)
            shopping_bids.append({
                'keyword': keyword['keyword'],
                'search_volume': keyword['search_volume'],
                'avg_bid': round(avg_bid, 2),
                'suggested_cpc': round(suggested_cpc, 2),
                'competition': keyword['competition'],
                'target_cpa': round(budget_per_keyword / max(expected_conversions, 1), 2)
            })
        shopping_bids.sort(key=lambda x: x['suggested_cpc'], reverse=True)
        return shopping_bids

    def export_results(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = os.getenv("SEM_OUTPUT_DIR")
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            def out(path):
                import os as _os
                return _os.path.join(output_dir, path)
        else:
            def out(path):
                return path
        search_campaign = self.create_search_campaign_keywords()
        search_filename = out(f"search_{timestamp}.csv")
        with open(search_filename, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Ad Group', 'Keyword', 'Search Volume', 'Match Type', 'Suggested CPC', 'Competition', 'Source', 'Intent', 'Reasoning'])
            for ad_group, keywords in search_campaign.items():
                for keyword in keywords:
                    writer.writerow([
                        ad_group,
                        keyword['keyword'],
                        keyword['search_volume'],
                        keyword['match_type'],
                        keyword['suggested_cpc'],
                        keyword['competition'],
                        keyword['source'],
                        keyword.get('intent',''),
                        keyword.get('reasoning','')
                    ])
        pmax_themes = self.create_pmax_themes()
        pmax_filename = out(f"pmax_{timestamp}.csv")
        with open(pmax_filename, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Theme Category', 'Theme'])
            for category, themes in pmax_themes.items():
                for theme in themes:
                    writer.writerow([category, theme])
        shopping_bids = self.calculate_shopping_cpc_bids()
        shopping_filename = out(f"shop_{timestamp}.csv")
        with open(shopping_filename, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Keyword', 'Search Volume', 'Avg Bid', 'Suggested CPC', 'Competition', 'Target CPA'])
            for bid in shopping_bids:
                writer.writerow([
                    bid['keyword'],
                    bid['search_volume'],
                    bid['avg_bid'],
                    bid['suggested_cpc'],
                    bid['competition'],
                    bid['target_cpa']
                ])
        return {
            'search_campaign': search_filename,
            'pmax_themes': pmax_filename,
            'shopping_bids': shopping_filename
        }

    def run_analysis(self):
        self.analyze_performance_indicators()
        results = self.export_results()
        return results

def main():
    try:
        explicit_keywords = os.getenv("SEM_KEYWORDS_FILE")
        if explicit_keywords and os.path.exists(explicit_keywords):
            latest_keywords_file = explicit_keywords
        else:
            import glob
            keyword_files = glob.glob("kw_*.csv") or glob.glob("wordstream_keywords_*.csv")
            if not keyword_files:
                print("No keyword files found. Please run the scraper first.")
                return 1
            latest_keywords_file = max(keyword_files)
        gemini_api_key = os.getenv("GEMINI_API_KEY")
        if not gemini_api_key or gemini_api_key == "your-gemini-api-key-here":
            raise RuntimeError("GEMINI_API_KEY not set. LLM is required.")
        analyzer = SEMAnalysis(latest_keywords_file, gemini_api_key=gemini_api_key)
        analyzer.run_analysis()
    except Exception as e:
        print(f"Error: {e}")
        return 1
    return 0

if __name__ == "__main__":
    exit(main()) 