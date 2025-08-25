--
-- PostgreSQL database dump
--

-- Dumped from database version 15.12 (Homebrew)
-- Dumped by pg_dump version 15.12 (Homebrew)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Data for Name: aegis_data_availability; Type: TABLE DATA; Schema: public; Owner: financeuser
--

INSERT INTO public.aegis_data_availability VALUES (55, 1, 'Royal Bank of Canada', 'RY', '{RBC,"Royal Bank",Royal,RY}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q1', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2024_Q1_pipeline');
INSERT INTO public.aegis_data_availability VALUES (56, 1, 'Royal Bank of Canada', 'RY', '{RBC,"Royal Bank",Royal,RY}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q2', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2024_Q2_pipeline');
INSERT INTO public.aegis_data_availability VALUES (57, 1, 'Royal Bank of Canada', 'RY', '{RBC,"Royal Bank",Royal,RY}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q3', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2024_Q3_pipeline');
INSERT INTO public.aegis_data_availability VALUES (58, 1, 'Royal Bank of Canada', 'RY', '{RBC,"Royal Bank",Royal,RY}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q4', '{transcripts,benchmarking,reports,rts}', '2025-08-24 18:50:53.147728', 'FY2024_Q4_pipeline');
INSERT INTO public.aegis_data_availability VALUES (59, 2, 'Toronto-Dominion Bank', 'TD', '{TD,"TD Bank","Toronto Dominion","TD Canada Trust"}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q1', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2024_Q1_pipeline');
INSERT INTO public.aegis_data_availability VALUES (60, 2, 'Toronto-Dominion Bank', 'TD', '{TD,"TD Bank","Toronto Dominion","TD Canada Trust"}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q2', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2024_Q2_pipeline');
INSERT INTO public.aegis_data_availability VALUES (61, 2, 'Toronto-Dominion Bank', 'TD', '{TD,"TD Bank","Toronto Dominion","TD Canada Trust"}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q3', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2024_Q3_pipeline');
INSERT INTO public.aegis_data_availability VALUES (62, 2, 'Toronto-Dominion Bank', 'TD', '{TD,"TD Bank","Toronto Dominion","TD Canada Trust"}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q4', '{transcripts,benchmarking,reports,rts}', '2025-08-24 18:50:53.147728', 'FY2024_Q4_pipeline');
INSERT INTO public.aegis_data_availability VALUES (63, 3, 'Bank of Montreal', 'BMO', '{BMO,"Bank of Montreal","Montreal Bank"}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q1', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2024_Q1_pipeline');
INSERT INTO public.aegis_data_availability VALUES (64, 3, 'Bank of Montreal', 'BMO', '{BMO,"Bank of Montreal","Montreal Bank"}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q2', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2024_Q2_pipeline');
INSERT INTO public.aegis_data_availability VALUES (65, 3, 'Bank of Montreal', 'BMO', '{BMO,"Bank of Montreal","Montreal Bank"}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q3', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2024_Q3_pipeline');
INSERT INTO public.aegis_data_availability VALUES (66, 3, 'Bank of Montreal', 'BMO', '{BMO,"Bank of Montreal","Montreal Bank"}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q4', '{transcripts,benchmarking,reports,rts}', '2025-08-24 18:50:53.147728', 'FY2024_Q4_pipeline');
INSERT INTO public.aegis_data_availability VALUES (67, 4, 'Bank of Nova Scotia', 'BNS', '{Scotia,Scotiabank,BNS,"Nova Scotia Bank"}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q1', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2024_Q1_pipeline');
INSERT INTO public.aegis_data_availability VALUES (68, 4, 'Bank of Nova Scotia', 'BNS', '{Scotia,Scotiabank,BNS,"Nova Scotia Bank"}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q2', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2024_Q2_pipeline');
INSERT INTO public.aegis_data_availability VALUES (69, 4, 'Bank of Nova Scotia', 'BNS', '{Scotia,Scotiabank,BNS,"Nova Scotia Bank"}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q3', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2024_Q3_pipeline');
INSERT INTO public.aegis_data_availability VALUES (70, 4, 'Bank of Nova Scotia', 'BNS', '{Scotia,Scotiabank,BNS,"Nova Scotia Bank"}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q4', '{transcripts,benchmarking,reports,rts}', '2025-08-24 18:50:53.147728', 'FY2024_Q4_pipeline');
INSERT INTO public.aegis_data_availability VALUES (71, 5, 'Canadian Imperial Bank of Commerce', 'CM', '{CIBC,CM,"Imperial Bank","Canadian Imperial"}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q1', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2024_Q1_pipeline');
INSERT INTO public.aegis_data_availability VALUES (72, 5, 'Canadian Imperial Bank of Commerce', 'CM', '{CIBC,CM,"Imperial Bank","Canadian Imperial"}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q2', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2024_Q2_pipeline');
INSERT INTO public.aegis_data_availability VALUES (73, 5, 'Canadian Imperial Bank of Commerce', 'CM', '{CIBC,CM,"Imperial Bank","Canadian Imperial"}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q3', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2024_Q3_pipeline');
INSERT INTO public.aegis_data_availability VALUES (74, 5, 'Canadian Imperial Bank of Commerce', 'CM', '{CIBC,CM,"Imperial Bank","Canadian Imperial"}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q4', '{transcripts,benchmarking,reports,rts}', '2025-08-24 18:50:53.147728', 'FY2024_Q4_pipeline');
INSERT INTO public.aegis_data_availability VALUES (75, 6, 'National Bank of Canada', 'NA', '{NBC,"National Bank",National,NA}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q1', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2024_Q1_pipeline');
INSERT INTO public.aegis_data_availability VALUES (76, 6, 'National Bank of Canada', 'NA', '{NBC,"National Bank",National,NA}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q2', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2024_Q2_pipeline');
INSERT INTO public.aegis_data_availability VALUES (77, 6, 'National Bank of Canada', 'NA', '{NBC,"National Bank",National,NA}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q3', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2024_Q3_pipeline');
INSERT INTO public.aegis_data_availability VALUES (78, 6, 'National Bank of Canada', 'NA', '{NBC,"National Bank",National,NA}', '{canadian_big_six,tier1_bank,canadian}', 2024, 'Q4', '{transcripts,benchmarking,reports,rts}', '2025-08-24 18:50:53.147728', 'FY2024_Q4_pipeline');
INSERT INTO public.aegis_data_availability VALUES (79, 1, 'Royal Bank of Canada', 'RY', '{RBC,"Royal Bank",Royal,RY}', '{canadian_big_six,tier1_bank,canadian}', 2025, 'Q1', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2025_Q1_pipeline');
INSERT INTO public.aegis_data_availability VALUES (80, 1, 'Royal Bank of Canada', 'RY', '{RBC,"Royal Bank",Royal,RY}', '{canadian_big_six,tier1_bank,canadian}', 2025, 'Q2', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2025_Q2_pipeline');
INSERT INTO public.aegis_data_availability VALUES (81, 1, 'Royal Bank of Canada', 'RY', '{RBC,"Royal Bank",Royal,RY}', '{canadian_big_six,tier1_bank,canadian}', 2025, 'Q3', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'FY2025_Q3_pipeline');
INSERT INTO public.aegis_data_availability VALUES (82, 2, 'Toronto-Dominion Bank', 'TD', '{TD,"TD Bank","Toronto Dominion","TD Canada Trust"}', '{canadian_big_six,tier1_bank,canadian}', 2025, 'Q1', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2025_Q1_pipeline');
INSERT INTO public.aegis_data_availability VALUES (83, 2, 'Toronto-Dominion Bank', 'TD', '{TD,"TD Bank","Toronto Dominion","TD Canada Trust"}', '{canadian_big_six,tier1_bank,canadian}', 2025, 'Q2', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2025_Q2_pipeline');
INSERT INTO public.aegis_data_availability VALUES (84, 2, 'Toronto-Dominion Bank', 'TD', '{TD,"TD Bank","Toronto Dominion","TD Canada Trust"}', '{canadian_big_six,tier1_bank,canadian}', 2025, 'Q3', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'FY2025_Q3_pipeline');
INSERT INTO public.aegis_data_availability VALUES (85, 3, 'Bank of Montreal', 'BMO', '{BMO,"Bank of Montreal","Montreal Bank"}', '{canadian_big_six,tier1_bank,canadian}', 2025, 'Q1', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2025_Q1_pipeline');
INSERT INTO public.aegis_data_availability VALUES (86, 3, 'Bank of Montreal', 'BMO', '{BMO,"Bank of Montreal","Montreal Bank"}', '{canadian_big_six,tier1_bank,canadian}', 2025, 'Q2', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2025_Q2_pipeline');
INSERT INTO public.aegis_data_availability VALUES (87, 3, 'Bank of Montreal', 'BMO', '{BMO,"Bank of Montreal","Montreal Bank"}', '{canadian_big_six,tier1_bank,canadian}', 2025, 'Q3', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'FY2025_Q3_pipeline');
INSERT INTO public.aegis_data_availability VALUES (88, 4, 'Bank of Nova Scotia', 'BNS', '{Scotia,Scotiabank,BNS,"Nova Scotia Bank"}', '{canadian_big_six,tier1_bank,canadian}', 2025, 'Q1', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2025_Q1_pipeline');
INSERT INTO public.aegis_data_availability VALUES (89, 4, 'Bank of Nova Scotia', 'BNS', '{Scotia,Scotiabank,BNS,"Nova Scotia Bank"}', '{canadian_big_six,tier1_bank,canadian}', 2025, 'Q2', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2025_Q2_pipeline');
INSERT INTO public.aegis_data_availability VALUES (90, 4, 'Bank of Nova Scotia', 'BNS', '{Scotia,Scotiabank,BNS,"Nova Scotia Bank"}', '{canadian_big_six,tier1_bank,canadian}', 2025, 'Q3', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'FY2025_Q3_pipeline');
INSERT INTO public.aegis_data_availability VALUES (91, 5, 'Canadian Imperial Bank of Commerce', 'CM', '{CIBC,CM,"Imperial Bank","Canadian Imperial"}', '{canadian_big_six,tier1_bank,canadian}', 2025, 'Q1', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2025_Q1_pipeline');
INSERT INTO public.aegis_data_availability VALUES (92, 5, 'Canadian Imperial Bank of Commerce', 'CM', '{CIBC,CM,"Imperial Bank","Canadian Imperial"}', '{canadian_big_six,tier1_bank,canadian}', 2025, 'Q2', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2025_Q2_pipeline');
INSERT INTO public.aegis_data_availability VALUES (93, 5, 'Canadian Imperial Bank of Commerce', 'CM', '{CIBC,CM,"Imperial Bank","Canadian Imperial"}', '{canadian_big_six,tier1_bank,canadian}', 2025, 'Q3', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'FY2025_Q3_pipeline');
INSERT INTO public.aegis_data_availability VALUES (94, 6, 'National Bank of Canada', 'NA', '{NBC,"National Bank",National,NA}', '{canadian_big_six,tier1_bank,canadian}', 2025, 'Q1', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2025_Q1_pipeline');
INSERT INTO public.aegis_data_availability VALUES (95, 6, 'National Bank of Canada', 'NA', '{NBC,"National Bank",National,NA}', '{canadian_big_six,tier1_bank,canadian}', 2025, 'Q2', '{transcripts,benchmarking,reports,rts,pillar3}', '2025-08-24 18:50:53.147728', 'FY2025_Q2_pipeline');
INSERT INTO public.aegis_data_availability VALUES (96, 6, 'National Bank of Canada', 'NA', '{NBC,"National Bank",National,NA}', '{canadian_big_six,tier1_bank,canadian}', 2025, 'Q3', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'FY2025_Q3_pipeline');
INSERT INTO public.aegis_data_availability VALUES (97, 7, 'JPMorgan Chase', 'JPM', '{JPM,"JP Morgan",Chase,JPMorgan}', '{us_bank,tier1_bank,bulge_bracket}', 2024, 'Q1', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2024_Q1_pipeline');
INSERT INTO public.aegis_data_availability VALUES (98, 7, 'JPMorgan Chase', 'JPM', '{JPM,"JP Morgan",Chase,JPMorgan}', '{us_bank,tier1_bank,bulge_bracket}', 2024, 'Q2', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2024_Q2_pipeline');
INSERT INTO public.aegis_data_availability VALUES (99, 7, 'JPMorgan Chase', 'JPM', '{JPM,"JP Morgan",Chase,JPMorgan}', '{us_bank,tier1_bank,bulge_bracket}', 2024, 'Q3', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2024_Q3_pipeline');
INSERT INTO public.aegis_data_availability VALUES (100, 7, 'JPMorgan Chase', 'JPM', '{JPM,"JP Morgan",Chase,JPMorgan}', '{us_bank,tier1_bank,bulge_bracket}', 2024, 'Q4', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2024_Q4_pipeline');
INSERT INTO public.aegis_data_availability VALUES (101, 8, 'Bank of America', 'BAC', '{BofA,BAC,"Bank of America",BoA}', '{us_bank,tier1_bank,bulge_bracket}', 2024, 'Q1', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2024_Q1_pipeline');
INSERT INTO public.aegis_data_availability VALUES (102, 8, 'Bank of America', 'BAC', '{BofA,BAC,"Bank of America",BoA}', '{us_bank,tier1_bank,bulge_bracket}', 2024, 'Q2', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2024_Q2_pipeline');
INSERT INTO public.aegis_data_availability VALUES (103, 8, 'Bank of America', 'BAC', '{BofA,BAC,"Bank of America",BoA}', '{us_bank,tier1_bank,bulge_bracket}', 2024, 'Q3', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2024_Q3_pipeline');
INSERT INTO public.aegis_data_availability VALUES (104, 8, 'Bank of America', 'BAC', '{BofA,BAC,"Bank of America",BoA}', '{us_bank,tier1_bank,bulge_bracket}', 2024, 'Q4', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2024_Q4_pipeline');
INSERT INTO public.aegis_data_availability VALUES (105, 9, 'Wells Fargo', 'WFC', '{Wells,WFC,"Wells Fargo"}', '{us_bank,tier1_bank}', 2024, 'Q1', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2024_Q1_pipeline');
INSERT INTO public.aegis_data_availability VALUES (106, 9, 'Wells Fargo', 'WFC', '{Wells,WFC,"Wells Fargo"}', '{us_bank,tier1_bank}', 2024, 'Q2', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2024_Q2_pipeline');
INSERT INTO public.aegis_data_availability VALUES (107, 9, 'Wells Fargo', 'WFC', '{Wells,WFC,"Wells Fargo"}', '{us_bank,tier1_bank}', 2024, 'Q3', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2024_Q3_pipeline');
INSERT INTO public.aegis_data_availability VALUES (108, 9, 'Wells Fargo', 'WFC', '{Wells,WFC,"Wells Fargo"}', '{us_bank,tier1_bank}', 2024, 'Q4', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2024_Q4_pipeline');
INSERT INTO public.aegis_data_availability VALUES (109, 10, 'Citigroup', 'C', '{Citi,Citibank,C}', '{us_bank,tier1_bank,bulge_bracket}', 2024, 'Q1', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2024_Q1_pipeline');
INSERT INTO public.aegis_data_availability VALUES (110, 10, 'Citigroup', 'C', '{Citi,Citibank,C}', '{us_bank,tier1_bank,bulge_bracket}', 2024, 'Q2', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2024_Q2_pipeline');
INSERT INTO public.aegis_data_availability VALUES (111, 10, 'Citigroup', 'C', '{Citi,Citibank,C}', '{us_bank,tier1_bank,bulge_bracket}', 2024, 'Q3', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2024_Q3_pipeline');
INSERT INTO public.aegis_data_availability VALUES (112, 10, 'Citigroup', 'C', '{Citi,Citibank,C}', '{us_bank,tier1_bank,bulge_bracket}', 2024, 'Q4', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2024_Q4_pipeline');
INSERT INTO public.aegis_data_availability VALUES (113, 7, 'JPMorgan Chase', 'JPM', '{JPM,"JP Morgan",Chase,JPMorgan}', '{us_bank,tier1_bank,bulge_bracket}', 2025, 'Q1', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2025_Q1_pipeline');
INSERT INTO public.aegis_data_availability VALUES (114, 7, 'JPMorgan Chase', 'JPM', '{JPM,"JP Morgan",Chase,JPMorgan}', '{us_bank,tier1_bank,bulge_bracket}', 2025, 'Q2', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2025_Q2_pipeline');
INSERT INTO public.aegis_data_availability VALUES (115, 8, 'Bank of America', 'BAC', '{BofA,BAC,"Bank of America",BoA}', '{us_bank,tier1_bank,bulge_bracket}', 2025, 'Q1', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2025_Q1_pipeline');
INSERT INTO public.aegis_data_availability VALUES (116, 8, 'Bank of America', 'BAC', '{BofA,BAC,"Bank of America",BoA}', '{us_bank,tier1_bank,bulge_bracket}', 2025, 'Q2', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2025_Q2_pipeline');
INSERT INTO public.aegis_data_availability VALUES (117, 9, 'Wells Fargo', 'WFC', '{Wells,WFC,"Wells Fargo"}', '{us_bank,tier1_bank}', 2025, 'Q1', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2025_Q1_pipeline');
INSERT INTO public.aegis_data_availability VALUES (118, 9, 'Wells Fargo', 'WFC', '{Wells,WFC,"Wells Fargo"}', '{us_bank,tier1_bank}', 2025, 'Q2', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2025_Q2_pipeline');
INSERT INTO public.aegis_data_availability VALUES (119, 10, 'Citigroup', 'C', '{Citi,Citibank,C}', '{us_bank,tier1_bank,bulge_bracket}', 2025, 'Q1', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2025_Q1_pipeline');
INSERT INTO public.aegis_data_availability VALUES (120, 10, 'Citigroup', 'C', '{Citi,Citibank,C}', '{us_bank,tier1_bank,bulge_bracket}', 2025, 'Q2', '{transcripts,reports}', '2025-08-24 18:50:53.147728', 'us_2025_Q2_pipeline');


--
-- Name: aegis_data_availability_id_seq; Type: SEQUENCE SET; Schema: public; Owner: financeuser
--

SELECT pg_catalog.setval('public.aegis_data_availability_id_seq', 120, true);


--
-- PostgreSQL database dump complete
--

