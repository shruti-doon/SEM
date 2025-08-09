#!/usr/bin/env python3
import os
import sys
import shutil
import glob
from datetime import datetime
from dotenv import load_dotenv
import subprocess

load_dotenv()

class SEMAnalysisPipeline:
    def __init__(self):
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_folder = f"output"
        self.keywords_file = None
        self.gemini_api_key = os.getenv("GEMINI_API_KEY")

    def create_output_folder(self):
        if os.path.exists(self.output_folder):
            shutil.rmtree(self.output_folder)
        os.makedirs(self.output_folder)

    def run_web_scraping(self):
        try:
            if not os.path.exists("config.yaml"):
                print("error:config_missing")
                return False
            env = os.environ.copy()
            env["SEM_OUTPUT_DIR"] = self.output_folder
            result = subprocess.run([sys.executable, "wordstream_scraper.py"], capture_output=True, text=True, env=env)
            if result.returncode != 0:
                print("error:scraper_failed")
                if result.stdout:
                    print(result.stdout.strip().splitlines()[-1])
                if result.stderr:
                    print(result.stderr.strip().splitlines()[-1])
                return False
            keyword_files = glob.glob(os.path.join(self.output_folder, "kw_*.csv")) or glob.glob(os.path.join(self.output_folder, "wordstream_keywords_*.csv"))
            if not keyword_files:
                print("error:no_keywords")
                return False
            self.keywords_file = max(keyword_files)
            return True
        except Exception as e:
            print(f"error:scraper_exception:{e}")
            return False

    def run_sem_analysis(self):
        try:
            if not self.keywords_file:
                print("error:no_keywords_for_analysis")
                return False
            env = os.environ.copy()
            env["SEM_OUTPUT_DIR"] = self.output_folder
            env["SEM_KEYWORDS_FILE"] = self.keywords_file
            result = subprocess.run([sys.executable, "sem_analysis.py"], capture_output=True, text=True, env=env)
            if result.returncode != 0:
                print("error:analysis_failed")
                if result.stdout:
                    print(result.stdout.strip().splitlines()[-1])
                if result.stderr:
                    print(result.stderr.strip().splitlines()[-1])
            return result.returncode == 0
        except Exception as e:
            print(f"error:analysis_exception:{e}")
            return False

    def collect_deliverables(self):
        files = [x for x in os.listdir(self.output_folder) if x.endswith('.csv')]
        if not files:
            print("error:no_deliverables")
            return False
        return True

    def run_pipeline(self):
        self.create_output_folder()
        if not self.run_web_scraping():
            return False
        if not self.run_sem_analysis():
            return False
        if not self.collect_deliverables():
            return False
        return True


def main():
    try:
        pipeline = SEMAnalysisPipeline()
        ok = pipeline.run_pipeline()
        if ok:
            print(pipeline.output_folder)
            return 0
        return 1
    except KeyboardInterrupt:
        print("error:interrupted")
        return 1
    except Exception as e:
        print(f"error:unexpected:{e}")
        return 1

if __name__ == "__main__":
    exit(main()) 