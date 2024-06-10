import random
import string

# Function to generate a random alphanumeric string of length 8
def generate_random_string(length):
  letters_and_digits = string.ascii_lowercase + string.digits
  return ''.join(random.choice(letters_and_digits) for i in range(length))

# List of possible elements for the second and fourth position
elements2 = ["gb", "de", "fr", "us", "jp", "au", "br", "mx", "ca", "it"]
elements4 = ["art", "ars", "aru", "do6", "jn1", "a7u", "gry", "mkf", "cla", "iik", "kll"]

# List of possible elements for the seventh position
elements7 = ["ces", "ads", "pov", "fax", "dvr", "iptv"]

# Function to generate a single entry
def generate_entry():
  # Generate random elements
  element1 = generate_random_string(8)
  element2 = random.choice(elements2)
  element3 = 0
  element4 = random.choice(elements4)
  element5 = element6 = 0  # Fixed vales

  element7 = random.choice(elements7)
  element8 = 12  # Fixed value

  # Return the formatted entry (using f-string might not be supported in 3.2)
  return "%s/%s/%d/%s/%d/%d/%s/%d/" % (element1, element2, element3, element4, element5, element6, element7, element8)

# Generate 40,000 entries
for _ in range(40000):
  entry = generate_entry()
  print(entry)
