# Check card: `members`

**State after milestone 3:** questions 1 to 6 and 8 are answered. Question 7 needs gates, and those are built in milestone 4.

## 1. What does one row mean?

One loyalty member: a person who signed up in the app. The key the app gives is `member_id`.

A row is about a **person**. Three of its columns are personal data: the name, the email and the phone number.

## 2. Who uses this table, and what do they decide with it?

| Who | What they decide | What goes wrong for them when the data is bad |
|---|---|---|
| Marketing | Who gets an email, and at which address | One person stored twice gets two emails. A wrong address reaches a stranger |
| The member | Nothing. They collect points | Their points are split over two accounts |

## 3. How is the data made? So what can go wrong?

Customers type it themselves, on a phone, standing in a shop.

| Step | Made by | What can go wrong |
|---|---|---|
| A customer signs up | A person on a phone | A typing mistake in the email. Capital letters. A space at the end |
| The same customer signs up again | The same person, months later | A second `member_id` for one person |
| A customer skips a box | A person | A half-filled row |

## 4. Which of those would hurt most?

| Rank | Worry | Why it is here |
|---|---|---|
| 1 | The same person twice | Two emails and two sets of points. Planted case 13 |
| 2 | An email that cannot be delivered | Marketing pays to send it, and nobody reads it |
| 3 | No answer to "may we email you?" | An email to someone who did not say yes |
| 4 | A member of a store that does not exist | Offers are sent by home store |
| 5 | An empty phone number | The app does not ask for it. **No check** |

One more worry is not about the rows. It is about the checks: a check prints its bad rows, and those rows hold a person's name and email.

## 5. What must be prepared before it can be checked?

| Worry | What must be prepared first |
|---|---|
| The same person twice | **Clean the value first.** Remove the spaces around the email and make it lower case. Then group by it |
| An email that cannot be delivered | Take it apart at the @ sign |
| A member of an unknown store | Join to `stores` |

`Priya.Nair@Example.com` and `priya.nair@example.com` are two different texts and one person. A unique check on the email as typed finds nothing.

## 6. The check

Four checks. The SQL is in `lotus_pipeline/other_checks.py`.

| # | Check | The limit | Lens | Question | How serious |
|---|---|---|---|---|---|
| 34 | One person is one member | Each cleaned email belongs to one member_id | Data | Unique? | Medium. Nobody is paid wrongly. A person merges the two accounts |
| 35 | An email has the shape of an email | One @, with text before it. A dot after it. No space inside | Data | Valid? | Medium |
| 36 | A member row has what marketing needs | A name, a home store, a join date, and a yes or no to marketing. A phone may be empty | Data | Complete? | High for the marketing answer |
| 37 | A member has a real store and a past join date | home_store_id is in the stores table. joined_on is not after the newest loaded day | Business | Consistent? | Low |

**No check returns a name, an email or a phone number.** A bad row is named by its `member_id`. A check's output is printed, logged and copied into other tables, and personal data must not travel with it. A test proves it for every check on this card.

## 7. What happens when it fails?

Not yet. Today a failed check changes nothing: the checks are run by hand, with `python -m lotus_pipeline inspect`. Milestone 4 decides for each one: stop, set the rows aside, or warn. And who is told.

## 8. The proof

| Fault | Caught by | A check that does not catch it, and why |
|---|---|---|
| Planted case 13: `M-0304` joins on 08-26 with the email of `M-0117`, in other letter case | 34 | A unique check on `member_id`, and one on `email` as typed: both are unique |
| An email with no @, with two, with a space, planted in a test | 35 | 34: the email belongs to one member |
| No name, and no answer to marketing, planted in a test | 36 | 35: the email is fine |
| A home store `S09`, a join date in 2027, planted in a test | 37 | 36: every cell is filled |
