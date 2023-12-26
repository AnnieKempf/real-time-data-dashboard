from faker import Faker

f = Faker(["sv_SE", "dk_DK", "no_NO", "fi_FI"])

print(f.name())
print(f.address())
