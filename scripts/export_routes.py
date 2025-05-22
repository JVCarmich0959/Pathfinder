#!/usr/bin/env python3
"""Export a planned route to HTML and PDF."""

from datetime import datetime
import folium
from pathfinder import plan_route, get_engine


OUTPUT_DIR = "maps"


def main():
    engine = get_engine()
    df = plan_route(engine=engine, limit=20)
    m = folium.Map(location=[df.lat.mean(), df.lon.mean()], zoom_start=6)
    folium.PolyLine(df[["lat", "lon"]].values, color="blue").add_to(m)
    for _, row in df.iterrows():
        folium.Marker([row.lat, row.lon], tooltip=f"{row.order}").add_to(m)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    html_path = f"{OUTPUT_DIR}/route_{timestamp}.html"
    pdf_path = f"{OUTPUT_DIR}/route_{timestamp}.pdf"
    m.save(html_path)
    try:
        import weasyprint

        weasyprint.HTML(html_path).write_pdf(pdf_path)
        print(f"📝  PDF saved to {pdf_path}")
    except Exception:
        print("PDF conversion skipped (weasyprint not installed)")
    print(f"🌐  HTML map saved to {html_path}")


if __name__ == "__main__":
    main()
