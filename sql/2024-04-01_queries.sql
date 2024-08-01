
-- Week 1: Dimensional Data Modeling, Dimensional Data Modeling Day 1 Lecture
SELECT * FROM schema.courses WHERE module = 'Week 1: Dimensional Data Modeling' AND slug = 'dimensional-data-modeling-day-1-lecture' AND version = 'version';
DELETE FROM schema.courses WHERE module = 'Week 1: Dimensional Data Modeling' AND slug = 'dimensional-data-modeling-day-1-lecture' AND version = 'version';
INSERT INTO schema.courses (
    product_ids,
    module,
    title,
    description,
    video_url,
    version,
    slug,
    github_url,
    slides_url,
    duration_seconds,
    has_query_editor,
    user_id
) VALUES (
    (SELECT array_agg(product_id) FROM schema.products WHERE description ILIKE '%version%' OR description ILIKE '%upgrade%')::TEXT[],
    'Week 1: Dimensional Data Modeling',
    'Dimensional Data Modeling Day 1 Lecture',
    'In this presentation, we explore the fundamental concepts essential for effective data modeling and database management. We dive into understanding your data consumer, the distinctions between OLTP and OLAP data modeling, the principles of Cumulative Table design, and the tradeoff between compactness and usability. We also address the challenges of temporal cardinality explosion and the potential pitfalls of run-length encoding compression. By the end of this lecture, you will have a strong foundation in the key concepts of data modeling and be prepared to apply these principles in practice. [Recorded Nov 7, 2023]',
    'secret_path/dimensional-data-modeling/day1/lecture/lecture.m3u8',
    'version',
    'dimensional-data-modeling-day-1-lecture',
    'https://github.com/bootcamp/tree/main/data-modeling-postgres',
    'secret_path/dimensional-data-modeling/day1/slides.pdf',
    3667,
    true,
    2
);
