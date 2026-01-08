"""
Premier League Season Scraper
Scrapes all matches from an entire season with matchweek information
"""
import time
import json
import re
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    WebDriverException,
)
from loguru import logger

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import SELENIUM_CONFIG, SCRAPING_CONFIG


class SeasonScraper:
    """
    Scraper for entire Premier League season with matchweek information.
    """
    
    # Season configurations - start_match_id is the first match ID of MW1
    # Note: IDs are sequential within each matchweek (MW1: start_id to start_id+9)
    # but jumps between seasons are not constant
    SEASONS = {
        "2025/26": {"season_id": 2025, "start_match_id": 2561895},
        "2024/25": {"season_id": 2024, "start_match_id": 2444470},
        "2023/24": {"season_id": 2023, "start_match_id": 2367538},
    }
    
    TOTAL_MATCHWEEKS = 38
    MATCHES_PER_WEEK = 10
    
    def __init__(self, headless: bool = True):
        """Initialize the season scraper."""
        self.headless = headless
        self.driver = None
        self.wait = None
        logger.info("Initializing Season Scraper")
    
    def _setup_driver(self) -> None:
        """Configure Chrome WebDriver."""
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument("--headless=new")
        
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument(f"--window-size={SELENIUM_CONFIG['window_size'][0]},{SELENIUM_CONFIG['window_size'][1]}")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.implicitly_wait(SELENIUM_CONFIG["implicit_wait"])
        self.driver.set_page_load_timeout(SELENIUM_CONFIG["page_load_timeout"])
        self.wait = WebDriverWait(self.driver, SELENIUM_CONFIG["implicit_wait"])
        
        logger.info("Chrome WebDriver initialized")
    
    def start(self) -> None:
        """Start the WebDriver session."""
        if self.driver is None:
            self._setup_driver()
    
    def stop(self) -> None:
        """Stop the WebDriver session."""
        if self.driver:
            self.driver.quit()
            self.driver = None
            logger.info("WebDriver session closed")
    
    def _handle_cookie_consent(self) -> None:
        """Handle cookie consent popup."""
        try:
            cookie_button = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#onetrust-accept-btn-handler"))
            )
            cookie_button.click()
            time.sleep(1)
        except TimeoutException:
            pass
        except Exception as e:
            logger.debug(f"Cookie consent: {e}")
    
    def _calculate_match_ids_for_matchweek(self, matchweek: int, season: str = "2025/26") -> List[int]:
        """
        Calculate match IDs for a specific matchweek based on start_match_id.
        
        Each matchweek has 10 matches. Match IDs are sequential.
        MW1 starts at start_match_id, MW2 starts at start_match_id + 10, etc.
        
        Args:
            matchweek: Matchweek number (1-38)
            season: Season string
            
        Returns:
            List of 10 match IDs for the matchweek
        """
        season_config = self.SEASONS.get(season)
        if not season_config:
            logger.error(f"Unknown season: {season}")
            return []
        
        start_match_id = season_config["start_match_id"]
        
        # Calculate starting match ID for this matchweek
        # MW1: start_match_id + 0
        # MW2: start_match_id + 10
        # MW3: start_match_id + 20
        # etc.
        mw_start_id = start_match_id + (matchweek - 1) * self.MATCHES_PER_WEEK
        
        match_ids = [mw_start_id + i for i in range(self.MATCHES_PER_WEEK)]
        
        logger.info(f"Calculated match IDs for MW{matchweek}: {match_ids[0]} to {match_ids[-1]}")
        return match_ids

    
    def get_matchweek_matches(self, matchweek: int, season: str = "2025/26") -> List[Dict[str, Any]]:
        """
        Get all match IDs and basic info for a specific matchweek.
        Uses Selenium to interact with filters.
        
        Args:
            matchweek: Matchweek number (1-38)
            season: Season string (e.g., "2025/26")
            
        Returns:
            List of dicts with match_id, home_team, away_team, matchweek
        """
        season_config = self.SEASONS.get(season)
        if not season_config:
            logger.error(f"Unknown season: {season}")
            return []
        
        url = f"https://www.premierleague.com/results"
        
        logger.info(f"Fetching Matchweek {matchweek} matches")
        
        try:
            self.driver.get(url)
            time.sleep(3)
            self._handle_cookie_consent()
            
            # Click on All Filters to access all filter options
            try:
                all_filters_btn = self.driver.find_element(
                    By.CSS_SELECTOR, 'button[aria-label="All Filters"]'
                )
                self.driver.execute_script("arguments[0].click();", all_filters_btn)
                time.sleep(1)
                
                # First, select the correct season
                season_id = season_config["season_id"]
                season_label = f"{season_id}/{str(season_id + 1)[-2:]}"  # e.g., "2024/25"
                logger.info(f"Selecting season: {season_label}")
                
                try:
                    # Find and click season option
                    season_options = self.driver.find_elements(
                        By.XPATH, 
                        "//label[contains(@class, 'input-button__label')]"
                    )
                    for opt in season_options:
                        if season_label in opt.text.strip():
                            self.driver.execute_script("arguments[0].scrollIntoView(true);", opt)
                            time.sleep(0.3)
                            self.driver.execute_script("arguments[0].click();", opt)
                            logger.info(f"Selected season: {opt.text.strip()}")
                            break
                    time.sleep(0.5)
                except Exception as e:
                    logger.warning(f"Could not select season: {e}")
                
                # Clear month filter by clicking "All" option for months
                try:
                    month_all = self.driver.find_element(
                        By.XPATH, "//label[contains(@class, 'input-button__label') and text()='All']"
                    )
                    self.driver.execute_script("arguments[0].click();", month_all)
                    time.sleep(0.5)
                except NoSuchElementException:
                    pass
                
                # Find and click the specific matchweek option
                mw_label_xpath = f"//label[contains(@class, 'input-button__label') and text()='MW{matchweek}']"
                try:
                    mw_option = self.wait.until(
                        EC.element_to_be_clickable((By.XPATH, mw_label_xpath))
                    )
                    self.driver.execute_script("arguments[0].click();", mw_option)
                    time.sleep(0.5)
                except TimeoutException:
                    # Try scrolling to find MW option
                    mw_options = self.driver.find_elements(
                        By.XPATH, 
                        "//label[contains(@class, 'input-button__label')]"
                    )
                    for opt in mw_options:
                        if opt.text.strip() == f"MW{matchweek}":
                            self.driver.execute_script("arguments[0].scrollIntoView(true);", opt)
                            time.sleep(0.3)
                            self.driver.execute_script("arguments[0].click();", opt)
                            break
                    time.sleep(0.5)
                
                # Click Save button
                save_btn = self.wait.until(
                    EC.element_to_be_clickable((By.XPATH, "//button[text()='Save']"))
                )
                self.driver.execute_script("arguments[0].click();", save_btn)
                time.sleep(3)  # Wait for results to update
                
                logger.info(f"Filter applied for {season} MW{matchweek}")
                
            except (TimeoutException, NoSuchElementException) as e:
                logger.warning(f"Could not apply filter via All Filters: {e}")
            
            # Extract match info using JavaScript
            extract_script = """
                const extractMatches = () => {
                    const matches = [];
                    
                    // Find all match cards
                    const matchCards = document.querySelectorAll('a.match-card, a[href*="/match/"]');
                    
                    matchCards.forEach(card => {
                        const href = card.getAttribute('href') || '';
                        const matchIdMatch = href.match(/match\\/(\d+)/);
                        
                        if (matchIdMatch) {
                            const matchId = parseInt(matchIdMatch[1]);
                            
                            // Get team names
                            const teamSelectors = [
                                '.mc-summary__team-name',
                                '[class*="team-name"]',
                                '.team-name'
                            ];
                            
                            let homeTeam = null, awayTeam = null;
                            for (const sel of teamSelectors) {
                                const teams = card.querySelectorAll(sel);
                                if (teams.length >= 2) {
                                    homeTeam = teams[0].textContent.trim();
                                    awayTeam = teams[1].textContent.trim();
                                    break;
                                }
                            }
                            
                            // Get score
                            const scoreEl = card.querySelector('.mc-summary__score, [class*="score"]');
                            let homeScore = null, awayScore = null;
                            if (scoreEl) {
                                const scoreText = scoreEl.textContent.trim();
                                const scoreMatch = scoreText.match(/(\\d+)\\s*[-–]\\s*(\\d+)/);
                                if (scoreMatch) {
                                    homeScore = parseInt(scoreMatch[1]);
                                    awayScore = parseInt(scoreMatch[2]);
                                }
                            }
                            
                            // Get date
                            const dateHeader = card.closest('.match-list-item')?.querySelector('.match-list-item__date');
                            const date = dateHeader?.textContent.trim() || null;
                            
                            // Avoid duplicates
                            if (!matches.some(m => m.match_id === matchId)) {
                                matches.push({
                                    match_id: matchId,
                                    home_team: homeTeam,
                                    away_team: awayTeam,
                                    home_score: homeScore,
                                    away_score: awayScore,
                                    date: date
                                });
                            }
                        }
                    });
                    
                    return matches;
                };
                
                return extractMatches();
            """
            
            matches = self.driver.execute_script(extract_script)
            
            # Add matchweek info to each match
            for match in matches:
                match["matchweek"] = matchweek
                match["season"] = season
            
            # Filter to only 10 matches max per matchweek
            matches.sort(key=lambda x: x['match_id'])
            if len(matches) > 10:
                matches = matches[:10]
            
            logger.info(f"Found {len(matches)} matches in Matchweek {matchweek}")
            return matches
            
        except Exception as e:
            logger.error(f"Error fetching matchweek {matchweek}: {e}")
            return []
    
    def scrape_match_with_matchweek(
        self, 
        match_id: int, 
        matchweek: int = None,
        season: str = None
    ) -> Optional[Dict[str, Any]]:
        """
        Scrape a single match with matchweek information.
        
        Args:
            match_id: Match ID
            matchweek: Matchweek number (will be extracted if not provided)
            season: Season string
            
        Returns:
            Match data dict with matchweek info
        """
        url = f"https://www.premierleague.com/match/{match_id}"
        logger.info(f"Scraping match {match_id}")
        
        try:
            self.driver.get(url)
            time.sleep(3)
            self._handle_cookie_consent()
            
            # Extract matchweek from page if not provided
            if matchweek is None:
                matchweek = self._extract_matchweek_from_page()
            
            # Extract match info and statistics
            match_data = self._extract_all_match_data()
            
            if match_data:
                match_data["match_id"] = match_id
                match_data["url"] = url
                match_data["matchweek"] = matchweek
                match_data["season"] = season
                match_data["scraped_at"] = datetime.now().isoformat()
                
                logger.info(f"Successfully scraped match {match_id} (MW{matchweek})")
                return match_data
            
            return None
            
        except Exception as e:
            logger.error(f"Error scraping match {match_id}: {e}")
            return None
    
    def _extract_matchweek_from_page(self) -> Optional[int]:
        """Extract matchweek number from the match page."""
        try:
            script = """
                const extractMW = () => {
                    // Try multiple selectors for matchweek
                    const selectors = [
                        '.match-header__gameweek',
                        '[class*="matchweek"]',
                        '[class*="gameweek"]',
                        '.mc-summary__info'
                    ];
                    
                    for (const sel of selectors) {
                        const el = document.querySelector(sel);
                        if (el) {
                            const text = el.textContent;
                            const match = text.match(/(?:Matchweek|MW|Gameweek|GW)\s*(\d+)/i);
                            if (match) {
                                return parseInt(match[1]);
                            }
                        }
                    }
                    
                    // Try from breadcrumb or page content
                    const pageText = document.body.innerText;
                    const match = pageText.match(/Matchweek\s+(\d+)/i);
                    if (match) {
                        return parseInt(match[1]);
                    }
                    
                    return null;
                };
                
                return extractMW();
            """
            
            return self.driver.execute_script(script)
            
        except Exception as e:
            logger.warning(f"Could not extract matchweek: {e}")
            return None
    
    def _extract_all_match_data(self) -> Optional[Dict[str, Any]]:
        """Extract all match data including stats - comprehensive version."""
        try:
            # First click on Stats tab
            self._click_stats_tab()
            
            # Scroll to load all sections
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
            
            script = """
                const extractAll = () => {
                    // Extract team names and score
                    const getMatchInfo = () => {
                        // Try multiple selectors for teams
                        const selectors = [
                            '.match-header__team-name',
                            '.team-name',
                            '[class*="team-name"]',
                            '.mc-summary__team-name'
                        ];
                        
                        let homeTeam = null, awayTeam = null;
                        for (const sel of selectors) {
                            const teams = document.querySelectorAll(sel);
                            if (teams.length >= 2) {
                                homeTeam = teams[0].textContent.trim();
                                awayTeam = teams[1].textContent.trim();
                                break;
                            }
                        }
                        
                        // Get score
                        let homeScore = null, awayScore = null;
                        const scoreEl = document.querySelector('.match-header__score, .score, [class*="score"]');
                        if (scoreEl) {
                            const text = scoreEl.textContent.trim();
                            const match = text.match(/(\\d+)\\s*[-–]\\s*(\\d+)/);
                            if (match) {
                                homeScore = parseInt(match[1]);
                                awayScore = parseInt(match[2]);
                            }
                        }
                        
                        // Get date
                        const dateEl = document.querySelector('.match-header__date, [class*="match-date"], time');
                        const date = dateEl?.textContent.trim() || null;
                        
                        // Get venue
                        const venueEl = document.querySelector('.match-header__venue, [class*="venue"]');
                        const venue = venueEl?.textContent.trim() || null;
                        
                        // Get referee
                        const refEl = document.querySelector('[class*="referee"]');
                        const referee = refEl?.textContent.replace('Referee:', '').trim() || null;
                        
                        return {
                            home_team: homeTeam,
                            away_team: awayTeam,
                            home_score: homeScore,
                            away_score: awayScore,
                            date: date,
                            venue: venue,
                            referee: referee
                        };
                    };
                    
                    // Extract all statistics (comprehensive)
                    const getStats = () => {
                        const stats = {};
                        
                        // Find all stat rows
                        const rows = document.querySelectorAll('.match-stats__table-row, [class*="stats-row"], [class*="stat-row"]');
                        
                        rows.forEach(row => {
                            // Get stat name
                            const nameEl = row.querySelector('.match-stats__stat-name, [class*="stat-name"], [class*="name"]');
                            if (!nameEl) return;
                            
                            const statName = nameEl.textContent.trim();
                            if (!statName) return;
                            
                            // Get home value
                            const homeEl = row.querySelector('.match-stats__table-cell--home, [class*="home"][class*="value"], td:first-child');
                            const awayEl = row.querySelector('.match-stats__table-cell--away, [class*="away"][class*="value"], td:last-child');
                            
                            // Handle different value formats
                            let homeValue = homeEl ? homeEl.textContent.trim() : null;
                            let awayValue = awayEl ? awayEl.textContent.trim() : null;
                            
                            // Clean up values (remove percentage signs for parsing)
                            const parseValue = (val) => {
                                if (!val) return null;
                                // Check if it contains a fraction like "5 (60%)"
                                const match = val.match(/^([\\d.]+)/);
                                return match ? match[1] : val;
                            };
                            
                            stats[statName] = {
                                home: homeValue,
                                away: awayValue,
                                home_parsed: parseValue(homeValue),
                                away_parsed: parseValue(awayValue)
                            };
                        });
                        
                        return stats;
                    };
                    
                    // Extract detailed stats by category
                    const getDetailedStats = () => {
                        const result = {
                            top_stats: {},
                            attack: {},
                            possession: {},
                            defence: {},
                            physical: {},
                            discipline: {}
                        };
                        
                        const categoryMap = {
                            'top stats': 'top_stats',
                            'attack': 'attack',
                            'possession': 'possession',
                            'defence': 'defence',
                            'defense': 'defence',
                            'physical': 'physical',
                            'discipline': 'discipline'
                        };
                        
                        // Extract all rows
                        const rows = document.querySelectorAll('.match-stats__table-row');
                        rows.forEach(row => {
                            const cells = row.querySelectorAll('.match-stats__table-cell');
                            if (cells.length >= 3) {
                                const home = cells[0].textContent.trim();
                                const name = cells[1].textContent.trim();
                                const away = cells[2].textContent.trim();
                                
                                if (name && !name.includes('undefined')) {
                                    const section = row.closest('[class*="section"]');
                                    let cat = 'top_stats';
                                    
                                    if (section) {
                                        const sectionText = section.textContent.toLowerCase();
                                        for (const [key, value] of Object.entries(categoryMap)) {
                                            if (sectionText.startsWith(key)) {
                                                cat = value;
                                                break;
                                            }
                                        }
                                    }
                                    
                                    result[cat][name] = {
                                        home: home,
                                        away: away
                                    };
                                }
                            }
                        });
                        
                        return result;
                    };
                    
                    return {
                        match_info: getMatchInfo(),
                        statistics: getStats(),
                        detailed_statistics: getDetailedStats()
                    };
                };
                
                return extractAll();
            """
            
            data = self.driver.execute_script(script)
            return data
            
        except Exception as e:
            logger.error(f"Error extracting match data: {e}")
            return None
    
    def _click_stats_tab(self) -> bool:
        """Click on the Stats tab."""
        try:
            stats_selectors = [
                "//a[contains(text(), 'Stats')]",
                "//button[contains(text(), 'Stats')]",
                "//*[@data-tab-index='3']",
            ]
            
            for selector in stats_selectors:
                try:
                    stats_tab = self.driver.find_element(By.XPATH, selector)
                    self.driver.execute_script("arguments[0].click();", stats_tab)
                    time.sleep(2)
                    return True
                except NoSuchElementException:
                    continue
            
            return False
            
        except Exception as e:
            logger.warning(f"Could not click Stats tab: {e}")
            return False
    
    def scrape_season(
        self,
        season: str = "2025/26",
        start_matchweek: int = 1,
        end_matchweek: int = 38,
        delay: float = 2.0
    ) -> List[Dict[str, Any]]:
        """
        Scrape all matches for specified matchweeks in a season.
        
        Args:
            season: Season string
            start_matchweek: Starting matchweek (1-38)
            end_matchweek: Ending matchweek (1-38)
            delay: Delay between requests in seconds
            
        Returns:
            List of all match data
        """
        all_matches = []
        
        try:
            self.start()
            
            for mw in range(start_matchweek, end_matchweek + 1):
                logger.info(f"Processing Matchweek {mw}/{end_matchweek}")
                
                # Get match list for this matchweek
                mw_matches = self.get_matchweek_matches(mw, season)
                
                for i, match_info in enumerate(mw_matches):
                    match_id = match_info["match_id"]
                    logger.info(f"  Match {i+1}/{len(mw_matches)}: {match_id}")
                    
                    # Scrape individual match
                    match_data = self.scrape_match_with_matchweek(
                        match_id=match_id,
                        matchweek=mw,
                        season=season
                    )
                    
                    if match_data:
                        all_matches.append(match_data)
                    
                    # Delay between matches
                    time.sleep(delay)
                
                logger.info(f"Completed Matchweek {mw}: {len(mw_matches)} matches")
            
        finally:
            self.stop()
        
        logger.info(f"Season scrape complete: {len(all_matches)} total matches")
        return all_matches
    
    def scrape_matchweeks(
        self,
        matchweeks: List[int],
        season: str = "2025/26",
        delay: float = 2.0
    ) -> List[Dict[str, Any]]:
        """
        Scrape specific matchweeks.
        
        Args:
            matchweeks: List of matchweek numbers to scrape
            season: Season string
            delay: Delay between requests
            
        Returns:
            List of match data
        """
        all_matches = []
        
        try:
            self.start()
            
            for mw in matchweeks:
                logger.info(f"Processing Matchweek {mw}")
                
                mw_matches = self.get_matchweek_matches(mw, season)
                
                for match_info in mw_matches:
                    match_id = match_info["match_id"]
                    
                    match_data = self.scrape_match_with_matchweek(
                        match_id=match_id,
                        matchweek=mw,
                        season=season
                    )
                    
                    if match_data:
                        all_matches.append(match_data)
                    
                    time.sleep(delay)
                
        finally:
            self.stop()
        
        return all_matches
    
    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


# Example usage
if __name__ == "__main__":
    import os
    
    # Configure logging
    os.makedirs("logs", exist_ok=True)
    logger.add("logs/season_scraper.log", rotation="10 MB")
    
    # Test with one matchweek
    scraper = SeasonScraper(headless=True)
    
    try:
        scraper.start()
        
        # Get matches from Matchweek 1
        matches = scraper.get_matchweek_matches(matchweek=1, season="2025/26")
        print(f"\nFound {len(matches)} matches in MW1:")
        for m in matches:
            print(f"  {m['match_id']}: {m['home_team']} vs {m['away_team']} (MW{m['matchweek']})")
        
        # Scrape first match with full stats
        if matches:
            first_match = matches[0]
            full_data = scraper.scrape_match_with_matchweek(
                match_id=first_match["match_id"],
                matchweek=1,
                season="2025/26"
            )
            print(f"\nFull match data:")
            print(json.dumps(full_data, indent=2, ensure_ascii=False))
            
    finally:
        scraper.stop()
