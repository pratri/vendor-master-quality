# Hard-case labeling guide

Each row in `data/labels/hard_case_candidates.csv` is a pair of vendor records. Decide whether AP
should treat them as one supplier. Use only what is in the row (names, addresses) plus general
knowledge of the organizations. The file deliberately has no UEI answer.

| Label | Use when | Examples |
|---|---|---|
| `duplicate` | Same legal entity. Paying either record pays the same company. Differences are spelling, punctuation, suffixes, truncation, word order, abbreviations, or a different address or suite of the same company. | "LEIDOS, INC." at two suites of one building. "DENVER, CITY & COUNTY OF" vs "CITY AND COUNTY OF DENVER". |
| `related` | Different legal entities in one group: parent and subsidiary, a university and its research foundation, a tribe and a tribally owned company, a state and one of its departments, a joint venture and a member. | "FLORIDA ATLANTIC UNIVERSITY" vs "FLORIDA ATLANTIC UNIVERSITY RESEARCH CORP". |
| `not_duplicate` | Different organizations that happen to look alike. | "WEST VIRGINIA UNIVERSITY" vs "WEST VIRGINIA STATE UNIVERSITY". "DIAMOND N I LLC" vs "DIAMOND N II LLC" (numbered series entities). |
| `unsure` | The row does not settle it, e.g. a common name in two distant cities with no other link, or a person's name next to a business name. | "JOHN SMITH" vs "SMITH CONSULTING". |

## Rules of thumb

- **Same name, different city.** Pick `duplicate` when the name is distinctive, such as a national
  company or a named firm. Pick `unsure` when it is generic ("A1 PLUMBING").
- **Government bodies.** A department is `related` to its state, not a `duplicate` of it. Two
  spellings of the same department are a `duplicate`.
- **Sole proprietors.** Where one record is a trade name and the other a person at the same
  address, use `unsure` unless the row shows they are the same.
- Put the reason in `notes` in a few words.

## Who labeled

All 150 pairs were labeled by Claude, an AI assistant, following this guide, without seeing the UEI
key. A person should spot-check them before anyone relies on these numbers.
