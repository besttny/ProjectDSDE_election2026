# Official Source Notes

Binary PDF files in this folder are local reference copies and are ignored by
Git. The project records source paths and source notes in the master CSV files
under `data/external/`.

Primary source for the Election 2026 working dataset:

- ECT Election 2026 PDFs listed in `configs/chaiyaphum_2_manifest.csv`
- Source URL recorded in the manifest: `https://www.ect.go.th/ect_th/th/election-2026`

Temporary reference files downloaded during master-data checks:

- `chaiyaphum2_candidate_5_official.pdf`
- `chaiyaphum_partylist_official.pdf`
- `chaiyaphum2_6_1_constituency_official.pdf`
- `chaiyaphum2_6_1_partylist_official.pdf`

Those temporary reference PDFs are not used as the current Election 2026 master
because the local Election 2026 result PDFs have the authoritative candidate
names for this project sample.

The `ส.ส. 6/1` and `ส.ส. 6/1 (บช.)` PDFs are official aggregate references
linked from the ECT QR announcement. They are used only to validate summed OCR
results through `data/external/aggregate_validation_reference.csv`; they must
not overwrite unit-level OCR values automatically.
