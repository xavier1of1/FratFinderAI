export function PageIntro({
  eyebrow,
  title,
  description,
  meta = []
}: {
  eyebrow: string;
  title: string;
  description: string;
  meta?: string[];
}) {
  return (
    <section className="pageIntro">
      <div className="pageIntroCopy">
        <p className="pageIntroEyebrow">{eyebrow}</p>
        <h2 className="pageIntroTitle">{title}</h2>
        <p className="pageIntroDescription">{description}</p>
      </div>
      {meta.length ? (
        <div className="pageIntroMeta" aria-label="Page summary">
          {meta.map((item) => (
            <span key={item} className="pageIntroMetaItem">
              {item}
            </span>
          ))}
        </div>
      ) : null}
    </section>
  );
}
