"""
Solar Events
"""

from yoyo import step

__depends__ = {'20211020_02_gTpZt-fill-up-reference-name-of-strings'}

steps = [
    step("""
    CREATE TABLE "solarevent" (
      "id" SERIAL PRIMARY KEY,
      "plant" INTEGER NOT NULL,
      "sunrise" TIMESTAMP WITH TIME ZONE NOT NULL,
      "sunset" TIMESTAMP WITH TIME ZONE NOT NULL
    );
    
    CREATE INDEX "idx_solarevent__plant" ON "solarevent" ("plant");
    
    ALTER TABLE "solarevent" ADD CONSTRAINT "fk_solarevent__plant" FOREIGN KEY ("plant") REFERENCES "plant" ("id") ON DELETE CASCADE;
    """,
    """
    DROP TABLE "solarevent"
    """)
]
