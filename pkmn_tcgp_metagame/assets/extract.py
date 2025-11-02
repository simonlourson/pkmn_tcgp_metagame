import asyncio
import csv
import json
import os
import re
from dataclasses import asdict, dataclass

import aiofile
import aiohttp
import dagster
from bs4 import BeautifulSoup
from dagster import DagsterLogManager

from pkmn_tcgp_metagame.assets import constants


@dataclass
class DeckListItem:
  url: str
  count: int


@dataclass
class Player:
  id: str
  name: str
  placing: str
  country: str
  decklist: list[DeckListItem]


@dataclass
class MatchResult:
  player_id: str
  score: int


@dataclass
class Match:
  match_results: list[MatchResult]


@dataclass
class Tournament:
  id: str
  name: str
  date: str
  organizer: str
  format: str
  nb_players: str
  players: list[Player]
  matches: list[Match]


@dataclass
class Card:
  url: str
  number: int
  name: str
  type: str
  subtype: str
  stage: str
  evolves_from: list[str]
  is_promo: bool


@dataclass
class Set:
  name: str
  code: str
  release_date: str
  url: str
  cards: list[Card]


# Create directory for a full file path if it does not already exists
def create_directory_for_file(path: str):
  try:
    directory = os.path.dirname(path)
    if directory and not os.path.exists(directory):
      os.makedirs(directory, exist_ok=True)
  except OSError as e:
    raise OSError(f"Failed to create directory for file {path}: {e}")
  except Exception as e:
    raise Exception(f"Unexpected error creating directory for file {path}: {e}")


# Extract the tr tags from a table, omiting the n first lines
def extract_trs(soup: BeautifulSoup, table_class: str, nb_headers: int = 1):
  try:
    table = soup.find(class_=table_class)
    if table is None:
      raise ValueError(f"Table with class '{table_class}' not found")

    trs = table.find_all("tr")
    if not trs:
      raise ValueError(f"No tr elements found in table with class '{table_class}'")

    # Remove headers safely
    if nb_headers > len(trs):
      raise ValueError(
        f"Cannot remove {nb_headers} headers from table with only {len(trs)} rows"
      )

    for _ in range(nb_headers):
      trs.pop(0)

    return trs
  except AttributeError as e:
    raise ValueError(f"Error parsing table structure: {e}")
  except Exception as e:
    raise ValueError(f"Unexpected error extracting table rows: {e}")


# Urls helpers
def construct_standings_url(tournament_id: str):
  return f"/tournament/{tournament_id}/standings?players"


def construct_pairings_url(tournament_id: str):
  return f"/tournament/{tournament_id}/pairings?players"


def construct_decklist_url(tournament_id: str, player_id: str):
  return f"/tournament/{tournament_id}/player/{player_id}/decklist"


regex_player_id = re.compile(r"/tournament/[a-zA-Z0-9_\-]*/player/[a-zA-Z0-9_]*")
regex_decklist_url = re.compile(
  r"/tournament/[a-zA-Z0-9_\-]*/player/[a-zA-Z0-9_]*/decklist"
)


# Extract a beautiful soup object from a url
async def async_soup_from_url(
  log: DagsterLogManager,
  session: aiohttp.ClientSession,
  sem: asyncio.Semaphore,
  url: str,
  use_cache: bool = True,
):
  if url is None:
    return None

  try:
    # Construct cache filename
    cache_filename = constants.BEAUTIFULSOUP_CACHE + url
    cache_filename = "".join(x for x in cache_filename if (x == "/" or x.isalnum()))
    cache_filename = f"{cache_filename}.html"

    html = ""

    if (
      use_cache
      and os.path.isfile(cache_filename)
      and os.path.getsize(cache_filename) > 0
    ):
      log.debug(f"url {url} is in cache")
      try:
        async with sem:
          async with aiofile.async_open(cache_filename, "r") as file:
            html = await file.read()
      except (OSError, IOError) as e:
        log.warning(f"Failed to read cache file {cache_filename}: {e}")
        html = ""

    if not html:
      log.debug(f"url {url} is not in cache, requesting from source")
      try:
        async with session.get(url) as resp:
          resp.raise_for_status()
          html = await resp.text()
      except aiohttp.ClientError as e:
        log.error(f"HTTP request failed for {url}: {e}")
        return None
      except asyncio.TimeoutError:
        log.error(f"HTTP request timed out for {url}")
        return None
      except Exception as e:
        log.error(f"Unexpected error during HTTP request for {url}: {e}")
        return None

      try:
        create_directory_for_file(cache_filename)
        async with sem:
          async with aiofile.async_open(cache_filename, "w") as file:
            await file.write(html)
      except (OSError, IOError) as e:
        log.warning(f"Failed to write cache file {cache_filename}: {e}")

    if not html:
      log.error(f"No HTML content available for {url}")
      return None

    try:
      return BeautifulSoup(html, "html.parser")
    except Exception as e:
      log.error(f"Failed to parse HTML for {url}: {e}")
      return None

  except Exception as e:
    log.error(f"Unexpected error in async_soup_from_url for {url}: {e}")
    return None


regex_card_name_url = re.compile(r"/cards\?q=name:")


async def extract_card(
  log: DagsterLogManager,
  session: aiohttp.ClientSession,
  sem: asyncio.Semaphore,
  url: str,
):
  soup = await async_soup_from_url(log, session, sem, url)
  if soup is None:
    raise ValueError(f"Failed to fetch or parse HTML for card URL: {url}")

  try:
    # Extract card number
    versions_table = soup.find("table", class_="card-prints-versions")
    if versions_table is None:
      raise ValueError(f"Card versions table not found for URL: {url}")

    current_tr = versions_table.find("tr", class_="current")
    if current_tr is None:
      raise ValueError(f"Current card version row not found for URL: {url}")

    number_span = current_tr.find("span", class_="prints-table-card-number")
    if number_span is None:
      raise ValueError(f"Card number span not found for URL: {url}")

    number_text = number_span.get_text()
    if not number_text or len(number_text) < 2:
      raise ValueError(f"Invalid card number format for URL: {url}")

    try:
      number = int(number_text[1:])
    except ValueError:
      raise ValueError(f"Cannot parse card number '{number_text[1:]}' for URL: {url}")

    # Check if promo
    tr_list = versions_table.find_all("tr")
    if len(tr_list) < 2:
      raise ValueError(f"Insufficient version rows for URL: {url}")

    first_tr_in_versions = tr_list[1]
    is_promo = first_tr_in_versions.attrs.get("class") is None

    # Extract card name
    name_span = soup.find("span", class_="card-text-name")
    if name_span is None:
      raise ValueError(f"Card name span not found for URL: {url}")
    name = name_span.text

    # Extract card type
    type_p = soup.find("p", class_="card-text-type")
    if type_p is None:
      raise ValueError(f"Card type paragraph not found for URL: {url}")

    text = list(type_p.stripped_strings)
    if not text:
      raise ValueError(f"No card type text found for URL: {url}")

    card_type = text[0].split("\n")[0]
    subtype = None
    stage = None
    evolves_from = None

    if card_type == "Pokémon":
      type_parts = text[0].split("- ")
      if len(type_parts) < 2:
        raise ValueError(f"Invalid Pokémon type format for URL: {url}")

      stage = type_parts[1].split("\n")[0]

      element = soup.find("p", class_="card-text-title")
      if element is None:
        raise ValueError(f"Card title element not found for Pokémon URL: {url}")

      element_strings = list(element.stripped_strings)
      if len(element_strings) < 2:
        raise ValueError(f"Insufficient title strings for Pokémon URL: {url}")

      subtype_parts = element_strings[1].split("- ")
      if len(subtype_parts) < 2:
        raise ValueError(f"Invalid subtype format for Pokémon URL: {url}")

      subtype = subtype_parts[1].strip()

      if stage != "Basic":
        evolves_from_link = soup.find("a", {"href": regex_card_name_url})
        if evolves_from_link is None:
          log.warning(f"Evolves from link not found for non-basic Pokémon URL: {url}")
          evolves_from = []
        else:
          evolves_from_url = evolves_from_link.attrs["href"]
          soup_evolves_from = await async_soup_from_url(
            log, session, sem, evolves_from_url
          )
          if soup_evolves_from is None:
            log.warning(f"Failed to fetch evolves from page for URL: {url}")
            evolves_from = []
          else:
            search_grid = soup_evolves_from.find("div", class_="card-search-grid")
            if search_grid is None:
              log.warning(
                f"Card search grid not found in evolves from page for URL: {url}"
              )
              evolves_from = []
            else:
              previous_stages_a = search_grid.find_all("a")
              evolves_from = [
                a.attrs["href"] for a in previous_stages_a if "href" in a.attrs
              ]

    elif card_type == "Trainer":
      type_parts = text[0].split("- ")
      if len(type_parts) < 2:
        raise ValueError(f"Invalid Trainer type format for URL: {url}")
      subtype = type_parts[1].split("\n")[0]

    return Card(url, number, name, card_type, subtype, stage, evolves_from, is_promo)

  except Exception as e:
    log.error(f"Error extracting card data from URL {url}: {e}")
    raise


# Extract the previous pairing pages urls
# This function assumes that the provided pairings page is the last of the tournament
def extract_previous_pairings_urls(pairings: BeautifulSoup):
  try:
    pairing_urls = pairings.find(class_="mini-nav")

    # If there is only one round, return empty array
    if pairing_urls is None:
      return []

    pairing_urls = pairing_urls.find_all("a")
    if not pairing_urls:
      return []

    # Pop the last item in array because it's the current page
    if len(pairing_urls) > 0:
      pairing_urls.pop(-1)

    pairing_urls = [a.attrs["href"] for a in pairing_urls if "href" in a.attrs]

    return pairing_urls
  except Exception as e:
    raise ValueError(f"Error extracting previous pairings URLs: {e}")


# Check if the pairing page is a bracket (single elimination)
def is_bracket_pairing(pairings: BeautifulSoup):
  try:
    return pairings.find("div", class_="live-bracket") is not None
  except Exception:
    return False


# Check if the pairing page is a table (swiss rounds)
regex_tournament_id = re.compile(r"[a-zA-Z0-9_\-]*")


def is_table_pairing(pairings: BeautifulSoup):
  try:
    pairings_div = pairings.find("div", class_="pairings")
    if pairings_div is not None:
      table = pairings_div.find("table", {"data-tournament": regex_tournament_id})
      if table is not None:
        return True
    return False
  except Exception:
    return False


# Return a list of matches from a bracket style pairing page
def extract_matches_from_bracket_pairings(pairings: BeautifulSoup):
  try:
    matches = []

    bracket_div = pairings.find("div", class_="live-bracket")
    if bracket_div is None:
      raise ValueError("Live bracket div not found")

    matches_div = bracket_div.find_all("div", class_="bracket-match")
    if not matches_div:
      return matches

    for match in matches_div:
      # We don't extract the match if one of the players is a bye
      if match.find("a", class_="bye") is not None:
        continue

      players_div = match.find_all("div", class_="live-bracket-player")
      if not players_div:
        continue

      match_results = []
      for player in players_div:
        if "data-id" not in player.attrs:
          raise ValueError("Player data-id attribute not found")

        score_div = player.find("div", class_="score")
        if score_div is None or "data-score" not in score_div.attrs:
          raise ValueError("Score div or data-score attribute not found")

        try:
          score = int(score_div.attrs["data-score"])
        except ValueError:
          raise ValueError(f"Cannot parse score '{score_div.attrs['data-score']}'")

        match_results.append(MatchResult(player.attrs["data-id"], score))

      if match_results:
        matches.append(Match(match_results))

    return matches
  except Exception as e:
    raise ValueError(f"Error extracting matches from bracket pairings: {e}")


# Return a list of matches from a table style pairing page
def extract_matches_from_table_pairings(pairings: BeautifulSoup):
  try:
    matches = []

    matches_tr = pairings.find_all("tr", {"data-completed": "1"})
    if not matches_tr:
      return matches

    for match in matches_tr:
      p1 = match.find("td", class_="p1")
      p2 = match.find("td", class_="p2")

      if p1 is not None and p2 is not None:
        if "data-id" not in p1.attrs:
          raise ValueError("Player 1 data-id attribute not found")
        if "data-id" not in p2.attrs:
          raise ValueError("Player 2 data-id attribute not found")
        if "data-count" not in p1.attrs:
          raise ValueError("Player 1 data-count attribute not found")
        if "data-count" not in p2.attrs:
          raise ValueError("Player 2 data-count attribute not found")

        try:
          p1_score = int(p1.attrs["data-count"])
          p2_score = int(p2.attrs["data-count"])
        except ValueError as e:
          raise ValueError(f"Cannot parse player scores: {e}")

        matches.append(
          Match(
            [
              MatchResult(p1.attrs["data-id"], p1_score),
              MatchResult(p2.attrs["data-id"], p2_score),
            ]
          )
        )

    return matches
  except Exception as e:
    raise ValueError(f"Error extracting matches from table pairings: {e}")


async def extract_set(
  log: DagsterLogManager,
  session: aiohttp.ClientSession,
  sem: asyncio.Semaphore,
  url: str,
):
  if url is None:
    return None

  try:
    log.info(f"extracting set {url}")
    soup = await async_soup_from_url(log, session, sem, url)
    if soup is None:
      raise ValueError(f"Failed to fetch or parse HTML for set URL: {url}")

    search_grid = soup.find("div", class_="card-search-grid")
    if search_grid is None:
      raise ValueError(f"Card search grid not found for set URL: {url}")

    cards_a = search_grid.find_all("a")
    if not cards_a:
      log.warning(f"No card links found for set URL: {url}")
      return []

    cards_urls = [a.attrs["href"] for a in cards_a if "href" in a.attrs]
    if not cards_urls:
      log.warning(f"No valid card URLs found for set URL: {url}")
      return []

    return await asyncio.gather(
      *[extract_card(log, session, sem, card_url) for card_url in cards_urls]
    )
  except Exception as e:
    log.error(f"Error extracting set from URL {url}: {e}")
    raise


async def extract_all_cards(log: DagsterLogManager):
  # Limit number of concurent http calls
  connector = aiohttp.TCPConnector(limit=20)

  # Limit number of concurent open files
  sem = asyncio.Semaphore(50)

  async with aiohttp.ClientSession(
    base_url=constants.BASE_URL_CARDS, connector=connector
  ) as session:
    soup = await async_soup_from_url(log, session, sem, "/cards", use_cache=False)
    trs = extract_trs(soup, "sets-table", 2)

    sets: list[Set] = []

    for tr in trs:
      tds = tr.find_all("td")
      set_url = tds[0].find("a").attrs["href"]
      set_release_date = tds[1].find("a").getText()
      set_code = tds[0].find("img").attrs["alt"]
      set_name = list(tds[0].stripped_strings)[0]

      sets.append(Set(set_name, set_code, set_release_date, set_url, None))

    sets_urls = [set.url for set in sets]
    sets_output_files = [
      f"{constants.JSON_OUTPUT}/sets/{set.code}.json" for set in sets
    ]

    for i in range(len(sets)):
      if os.path.isfile(sets_output_files[i]):
        sets_urls[i] = None
        log.info(
          f"skipping set {sets[i].code} because {sets_output_files[i]} already exists"
        )

    cards = await asyncio.gather(
      *[extract_set(log, session, sem, url) for url in sets_urls]
    )

    for i in range(len(sets)):
      if cards[i] is None:
        continue

      sets[i].cards = cards[i]

      output_file = f"{constants.JSON_OUTPUT}/sets/{sets[i].code}.json"

      try:
        create_directory_for_file(output_file)
        with open(output_file, "w") as f:
          json.dump(asdict(sets[i]), f, indent=2)
      except (OSError, IOError) as e:
        log.error(f"Failed to write set file {output_file}: {e}")
        raise
      except Exception as e:
        log.error(f"Unexpected error writing set file {output_file}: {e}")
        raise

    return dagster.MaterializeResult(
      metadata={
        "Number of files": dagster.MetadataValue.int(len(sets)),
      }
    )


# Return a list of DeckListItems from a player decklist page
regex_card_url = re.compile(r"pocket\.limitlesstcg\.com/cards/.*")


def extract_decklist(decklist: BeautifulSoup) -> list[DeckListItem]:
  try:
    decklist_div = decklist.find("div", class_="decklist")
    cards = []
    if decklist_div is not None:
      cards_a = decklist_div.find_all("a", {"href": regex_card_url})
      for card in cards_a:
        if "href" not in card.attrs:
          raise ValueError("Card link missing href attribute")

        card_text = card.text
        if not card_text:
          raise ValueError("Card link has no text content")

        try:
          count = int(card_text[0])
        except (ValueError, IndexError):
          raise ValueError(f"Cannot parse card count from text '{card_text}'")

        cards.append(
          DeckListItem(
            str(card.attrs["href"]).removeprefix(constants.BASE_URL_CARDS),
            count,
          )
        )

    return cards
  except Exception as e:
    raise ValueError(f"Error extracting decklist: {e}")


async def extract_players(
  log: DagsterLogManager,
  session: aiohttp.ClientSession,
  sem: asyncio.Semaphore,
  standings_page: BeautifulSoup,
  tournament_id: str,
) -> list[Player]:
  players = []
  player_trs = extract_trs(standings_page, "striped")
  player_ids = [
    player_tr.find("a", {"href": regex_player_id}).attrs["href"].split("/")[4]
    for player_tr in player_trs
  ]
  has_decklist = [
    player_tr.find("a", {"href": regex_decklist_url}) is not None
    for player_tr in player_trs
  ]
  player_names = [player_tr.attrs["data-name"] for player_tr in player_trs]
  player_placings = [
    player_tr.attrs.get("data-placing", -1) for player_tr in player_trs
  ]
  player_countries = [
    player_tr.attrs.get("data-country", None) for player_tr in player_trs
  ]

  decklist_urls = []
  for i in range(len(player_ids)):
    decklist_urls.append(
      construct_decklist_url(tournament_id, player_ids[i]) if has_decklist[i] else None
    )

  player_decklists = await asyncio.gather(
    *[async_soup_from_url(log, session, sem, url, True) for url in decklist_urls]
  )

  players = []
  for i in range(len(player_ids)):
    if player_decklists[i] is None:
      continue

    players.append(
      Player(
        player_ids[i],
        player_names[i],
        player_placings[i],
        player_countries[i],
        extract_decklist(player_decklists[i]),
      )
    )

  return players


async def extract_matches(
  log: DagsterLogManager,
  session: aiohttp.ClientSession,
  sem: asyncio.Semaphore,
  tournament_id: str,
) -> list[Match]:
  matches = []
  last_pairings = await async_soup_from_url(
    log, session, sem, construct_pairings_url(tournament_id)
  )
  previous_pairings_urls = extract_previous_pairings_urls(last_pairings)
  pairings = await asyncio.gather(
    *[async_soup_from_url(log, session, sem, url) for url in previous_pairings_urls]
  )
  pairings.append(last_pairings)

  for pairing in pairings:
    if is_bracket_pairing(pairing):
      matches = matches + extract_matches_from_bracket_pairings(pairing)
    elif is_table_pairing(pairing):
      matches = matches + extract_matches_from_table_pairings(pairing)
    else:
      raise Exception("Unrecognized pairing type")

  return matches


async def extract_standings(
  log: DagsterLogManager,
  session: aiohttp.ClientSession,
  sem: asyncio.Semaphore,
  standings_page: BeautifulSoup,
  tournament_id: str,
  tournament_name: str,
  tournament_date: str,
  tournament_organizer: str,
  tournament_format: str,
  tournament_nb_players: int,
):
  if standings_page is None:
    log.debug("skipping because tournament is already in output")
    return

  log.debug(f"extracting tournament {tournament_id}")

  players = await extract_players(log, session, sem, standings_page, tournament_id)
  if len(players) == 0:
    log.debug("skipping because no decklist was detected")
    return

  output_file = f"{constants.TOURNAMENTS_OUTPUT_DIR}/{tournament_id}.json"
  try:
    create_directory_for_file(output_file)
  except Exception as e:
    log.error(f"Failed to create directory for tournament file {output_file}: {e}")
    raise

  nb_decklists = 0
  for player in players:
    if len(player.decklist) > 0:
      nb_decklists += 1

  matches = await extract_matches(log, session, sem, tournament_id)

  tournament = Tournament(
    tournament_id,
    tournament_name,
    tournament_date,
    tournament_organizer,
    tournament_format,
    tournament_nb_players,
    players,
    matches,
  )

  try:
    with open(output_file, "w") as f:
      json.dump(asdict(tournament), f, indent=2)
  except (OSError, IOError) as e:
    log.error(f"Failed to write tournament file {output_file}: {e}")
    raise
  except Exception as e:
    log.error(f"Unexpected error writing tournament file {output_file}: {e}")
    raise


async def extract_tournament_list(
  log: DagsterLogManager,
  session: aiohttp.ClientSession,
  sem: asyncio.Semaphore,
  url: str,
):
  soup = await async_soup_from_url(log, session, sem, url, False)

  current_page = int(soup.find("ul", class_="pagination").attrs["data-current"])
  max_page = int(soup.find("ul", class_="pagination").attrs["data-max"])

  log.info(f"extracting completed tournaments page {current_page}")

  tournament_trs = extract_trs(soup, "completed-tournaments")
  tournament_ids = [
    tournament_tr.find("a", {"href": regex_standings_url}).attrs["href"].split("/")[2]
    for tournament_tr in tournament_trs
  ]
  tournament_names = [
    tournament_tr.attrs["data-name"] for tournament_tr in tournament_trs
  ]
  tournament_dates = [
    tournament_tr.attrs["data-date"] for tournament_tr in tournament_trs
  ]
  tournament_organizers = [
    tournament_tr.attrs["data-organizer"] for tournament_tr in tournament_trs
  ]
  tournament_formats = [
    tournament_tr.attrs["data-format"] for tournament_tr in tournament_trs
  ]
  tournament_nb_players = [
    tournament_tr.attrs["data-players"] for tournament_tr in tournament_trs
  ]

  standings_urls = [
    construct_standings_url(tournament_id) for tournament_id in tournament_ids
  ]

  for i in range(len(tournament_ids)):
    output_file = f"{constants.TOURNAMENTS_OUTPUT_DIR}/{tournament_ids[i]}.json"
    if os.path.isfile(output_file):
      standings_urls[i] = None

  # Get all standings page asynchroneously
  standings = await asyncio.gather(
    *[async_soup_from_url(log, session, sem, url) for url in standings_urls]
  )

  await asyncio.gather(
    *[
      extract_standings(
        log,
        session,
        sem,
        standings[i],
        tournament_ids[i],
        tournament_names[i],
        tournament_dates[i],
        tournament_organizers[i],
        tournament_formats[i],
        tournament_nb_players[i],
      )
      for i in range(len(tournament_ids))
    ]
  )

  # for i in range(len(tournament_ids)):
  #   await extract_standings(log, session, sem, standings[i], tournament_ids[i], tournament_names[i], tournament_dates[i], tournament_organizers[i], tournament_formats[i], tournament_nb_players[i])

  if current_page < max_page:
    await extract_tournament_list(
      log, session, sem, f"{first_tournament_page}&page={current_page + 1}"
    )


first_tournament_page = (
  "/tournaments/completed?game=POCKET&format=STANDARD&platform=all&type=online&time=all"
)
regex_standings_url = re.compile(r"/tournament/[a-zA-Z0-9_\-]*/standings")


async def extract_all_tournaments(log: DagsterLogManager):
  # Limit number of concurent http calls
  connector = aiohttp.TCPConnector(limit=20)

  # Limit number of concurent open files
  sem = asyncio.Semaphore(50)

  async with aiohttp.ClientSession(
    base_url=constants.BASE_URL_TOURNAMENTS, connector=connector
  ) as session:
    await extract_tournament_list(log, session, sem, first_tournament_page)


@dagster.asset(
  group_name="extract",
  kinds=["python", "json"],
)
async def set_files(
  context: dagster.AssetExecutionContext,
) -> dagster.MaterializeResult:
  """The raw JSON files containing all the cards for each set"""

  return await extract_all_cards(context.log)


@dagster.asset(
  group_name="extract",
  kinds=["python", "json"],
)
async def tournament_files(
  context: dagster.AssetExecutionContext,
) -> dagster.MaterializeResult:
  """The raw JSON files containing all the tournament data"""

  await extract_all_tournaments(context.log)

  number_of_files = len(
    [
      name
      for name in os.listdir(constants.TOURNAMENTS_OUTPUT_DIR)
      if os.path.isfile(os.path.join(constants.TOURNAMENTS_OUTPUT_DIR, name))
    ]
  )
  return dagster.MaterializeResult(
    metadata={
      "Number of files": dagster.MetadataValue.int(number_of_files),
    }
  )


regex_extract_between_square_brackets = re.compile(r"\[(.*)\]")


def translate_extension_code(full_name: str):
  try:
    if "Promo-A" in full_name:
      return "P-A"
    elif "Promo-B" in full_name:
      return "P-B"
    else:
      match = re.search(regex_extract_between_square_brackets, full_name)
      if match is None:
        raise ValueError(f"No square brackets found in extension name: {full_name}")
      return match.group(1)
  except Exception as e:
    raise ValueError(f"Error translating extension code for '{full_name}': {e}")


regex_extension_url = re.compile(r"/extensions/[a-zA-Z0-9\-]*.html")


@dagster.asset(
  group_name="extract",
  kinds=["python", "csv"],
)
async def translation_files(
  context: dagster.AssetExecutionContext,
) -> dagster.MaterializeResult:
  """The raw CSV files containing translations for each card"""

  # Limit number of concurent http calls
  connector = aiohttp.TCPConnector(limit=20)

  # Limit number of concurent open files
  sem = asyncio.Semaphore(50)

  async with aiohttp.ClientSession(
    base_url=constants.BASE_URL_TRANSLATIONS, connector=connector
  ) as session:
    soup = await async_soup_from_url(
      context.log, session, sem, "/jeux/mobile/pocket/cartodex/extensions.html", False
    )
    sets_a = soup.find_all("a", {"href": regex_extension_url})
    sets_ids = [translate_extension_code(a.get_text()) for a in sets_a]
    sets_urls = [a["href"] for a in sets_a]
    sets_soups = await asyncio.gather(
      *[async_soup_from_url(context.log, session, sem, url, True) for url in sets_urls]
    )
    translation_output_file = f"{constants.JSON_OUTPUT}/translations/fr.csv"

    card_translations = []

    for i in range(len(sets_ids)):
      cards_a = (
        sets_soups[i].find("div", {"id": "liste_cartes"}).find_all("a", class_="carte")
      )
      for a in cards_a:
        card_translations.append(
          [
            sets_ids[i],
            int(
              a.find("div", class_="carte_rarete").find("div").getText().split(" / ")[0]
            ),
            a.find("div", class_="carte_nom").get_text(strip=True),
          ]
        )

    try:
      create_directory_for_file(translation_output_file)
      with open(translation_output_file, "w") as f:
        write = csv.writer(f)
        write.writerows(card_translations)
    except (OSError, IOError) as e:
      context.log.error(
        f"Failed to write translation file {translation_output_file}: {e}"
      )
      raise
    except Exception as e:
      context.log.error(
        f"Unexpected error writing translation file {translation_output_file}: {e}"
      )
      raise

  return dagster.MaterializeResult(
    metadata={
      "Number of lines": dagster.MetadataValue.int(len(card_translations)),
    }
  )
