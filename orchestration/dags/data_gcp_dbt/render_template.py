from jinja2 import Environment, FileSystemLoader

# Load Jinja template
env = Environment(loader=FileSystemLoader("."))
template = env.get_template("models/mart/global/semantic/bookings_semantic.yml.j2")

# Define booking categories
booking_categories = [
    {"name": "LIVRE", "value_expr": "livre"},
    {"name": "CINEMA", "value_expr": "cinema"},
    {"name": "MUSIQUE_LIVE", "value_expr": "concert"},
    {"name": "SPECTACLE", "value_expr": "spectacle_vivant"},
    {"name": "MUSEE", "value_expr": "musee"},
    {"name": "PRATIQUE_ART", "value_expr": "pratique_artistique"},
    {"name": "INSTRUMENT", "value_expr": "instrument"},
]

# Render YAML
yaml_content = template.render(booking_categories=booking_categories)

# Save to a file
with open("models/mart/global/semantic/bookings_semantic.yml", "w") as f:
    f.write(yaml_content)
