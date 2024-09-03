import re
import ujson as json


tests = [
	r"\u0000",
	r"a\u0000",
	r"\0",
	"\x00test",
	r"\0testing",
	r"a\u0000b",
	r"\u0000\u0000",
	r"\\u0000a",
	r"\\u0000",
	r"a\\u0000",
]


json_tests = [
	r"""{"archived":false,"subreddit":"startups","author_flair_css_class":null,"controversiality":0,"gilded":0,"author_flair_text":null,"score_hidden":false,"retrieved_on":1424633238,"distinguished":null,"score":4,"parent_id":"t1_cont3y1","link_id":"t3_2w5kh7","body":"Internet music discovery is dominated by platforms expanding their music libraries as quickly as possible, creating an information overload and decision paralysis. It's really hard to find the newest, best music simply because they offer too many songs. Noon Pacific serves up 10 songs each week for you to explore and listen to as if it were a vinyl record. \n\nAlso, almost every music discovery tool I know of is influenced by record companies or money-backed singles so the charts are really skewed.\n\nCurrent monetization is via the mobile apps at $1.99 for iOS + Android.\n\u0000","author":"cdinnison","id":"conuz8i","name":"t1_conuz8i","created_utc":"1424150044","subreddit_id":"t5_2qh26","edited":false,"downs":0,"ups":4}""",
	r"""{"controversiality":0,"edited":false,"archived":false,"parent_id":"t1_con72jc","author":"herpy","body":"Thank you.  I buried my dreams along time ago before I figured out all this stuff about ADHD and found this sub.  But after finding it and reading stories like the one in this post I felt like those dreams were trying to rise from the grave I put them  in because maybe I was wrong and there was another way.  I think you just helped seal them for good.  Damn zombie dreams.\u0000","ups":1,"downs":0,"retrieved_on":1424622706,"distinguished":null,"created_utc":"1424110059","name":"t1_con8u0k","id":"con8u0k","author_flair_text":null,"subreddit_id":"t5_2qnwb","gilded":0,"author_flair_css_class":null,"score":1,"subreddit":"ADHD","score_hidden":false,"link_id":"t3_2w0iw2"}""",
	r"""{"name":"t1_co98h70","link_id":"t3_2ukm41","subreddit":"cs50","body":"It will be graded by check50 so if you submit it, you won't pass.\n\nLook at your error messages.  Notice how you are returning something at the end of your output?  `\\u0000` ?  That's an unprintable char which is why you don't see it on your output.  Check your loops again to make sure you aren't printing one too many characters...","author":"delipity","controversiality":0,"downs":0,"created_utc":"1422920336","author_flair_text":"alum","distinguished":null,"parent_id":"t1_co97tks","id":"co98h70","score":3,"archived":false,"ups":3,"subreddit_id":"t5_2s3yg","retrieved_on":1424247157,"edited":false,"gilded":0,"score_hidden":false,"author_flair_css_class":null}""",
	r"""{"archived":false,"created_utc":"1422878204","author_flair_text":null,"score_hidden":false,"subreddit_id":"t5_2t1jq","controversiality":0,"gilded":0,"edited":false,"author":"OldNedder","subreddit":"javahelp","distinguished":null,"body":"close - you probably need \"r &lt; height\" to go through the whole grid.  It seems like the longColumn parameter is unnecessary.  width and height can be obtained from the grid itself (unless you are allocating a grid with unused space).\n\nAlso \\u0000 should be '\\u0000'.  Or you can just compare the character with the integer 0:  if (grid[r][c] != 0) {}","author_flair_css_class":null,"score":1,"parent_id":"t1_co83juk","name":"t1_co8n1jx","id":"co8n1jx","link_id":"t3_2udofx","ups":1,"retrieved_on":1424257326,"downs":0}"""

]


r = re.compile(r"(?<!\\)\\u0000")


print("Tests1\n------")
for t in tests:
	print(repr(t), t,  r.sub("!", t))


print("\n\n\nTests2\n----------")
for t in tests:
	print(repr(t), t, t.replace("\\u0000", "\\\\u0000").replace("\0", "<<NULL>>"))


print("\n\n\nJSON Tests\n--------------")
for t in json_tests:
	s = r.sub(" ", t)
	print("-" * 20, "\n" * 2)
	print("original:\n\n", t, "\n")
	print("subbed:\n\n", repr(s), "\n")
