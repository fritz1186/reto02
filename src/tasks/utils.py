def validate_ruc(ruc):
    return (len(ruc) == 11 and ruc.isdigit())

def handle_invalid_ruc (ruc):
    with open ("invalid_rucs.text", "a") as f:
        f.write (f"{ruc}\n")