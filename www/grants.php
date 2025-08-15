<?php
require_once 'db_config.php'; // contains $host, $user, $pass, $dbname

$conn = new mysqli($host, $user, $pass, $dbname);
if ($conn->connect_error) {
    die("Connection failed: " . $conn->connect_error);
}

// Get periods
$periods = [];
$period_labels = [];
$result = $conn->query("SELECT id, period_label, period_end_date FROM reporting_periods ORDER BY period_end_date");
while ($row = $result->fetch_assoc()) {
    $periods[$row['id']] = $row['period_label'];
    $period_labels[] = $row['period_label'];
}

// Fetch funding_summary data
$summary = [];
$res = $conn->query("SELECT * FROM funding_summary ORDER BY period_id, category");
while ($row = $res->fetch_assoc()) {
    $summary[$row['category']][$periods[$row['period_id']]] = $row;
}

// Fetch education_awards data
$education = [];
$res = $conn->query("SELECT * FROM education_awards ORDER BY period_id");
while ($row = $res->fetch_assoc()) {
    $education[$periods[$row['period_id']]] = $row;
}
$conn->close();
?>

<!DOCTYPE html>
<html>
<head>
    <title>Funding Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        table { border-collapse: collapse; margin-bottom: 30px; }
        th, td { padding: 6px 12px; border: 1px solid #ccc; text-align: right; }
        th:first-child, td:first-child { text-align: left; }
    </style>
</head>
<body>
    <h1>Funding Summary</h1>

    <?php foreach ($summary as $category => $rows): ?>
        <h2><?= htmlspecialchars($category) ?></h2>
        <table>
            <thead>
                <tr>
                    <th>Metric</th>
                    <?php foreach ($period_labels as $label): ?>
                        <th><?= htmlspecialchars($label) ?></th>
                    <?php endforeach; ?>
                </tr>
            </thead>
            <tbody>
                <?php
                $metrics = [
                    "total_direct_costs" => "Total Direct Costs",
                    "peer_reviewed_direct_costs" => "Peer-Reviewed Costs",
                    "nci_direct_costs" => "NCI Direct Costs",
                    "percent_nci_of_peer_reviewed" => "% NCI of Peer",
                    "r01_investigators" => "# R01 Investigators",
                    "r01_awards" => "# R01 Awards",
                    "complex_grants" => "# Complex Grants",
                    "percent_complex_grants" => "% Complex Grants",
                    "multi_institutional_grants" => "# Multi-Institutional Grants",
                    "percent_multi_institutional" => "% Multi-Institutional"
                ];
                foreach ($metrics as $field => $label):
                ?>
                <tr>
                    <td><?= $label ?></td>
                    <?php foreach ($period_labels as $p): ?>
                        <td><?= isset($rows[$p][$field]) ? number_format($rows[$p][$field], 0) : "" ?></td>
                    <?php endforeach; ?>
                </tr>
                <?php endforeach; ?>
            </tbody>
        </table>

        <canvas id="chart_<?= $category ?>"></canvas>
        <script>
            const ctx_<?= $category ?> = document.getElementById("chart_<?= $category ?>").getContext("2d");
            new Chart(ctx_<?= $category ?>, {
                type: 'line',
                data: {
                    labels: <?= json_encode($period_labels) ?>,
                    datasets: [{
                        label: "<?= $category ?>: Total Direct Costs",
                        data: <?= json_encode(array_map(function($p) use ($rows) {
                            return isset($rows[$p]) ? floatval($rows[$p]['total_direct_costs']) : null;
                        }, $period_labels)) ?>,
                        borderColor: "blue",
                        fill: false
                    }]
                }
            });
        </script>
    <?php endforeach; ?>

    <h2>Education Awards</h2>
    <table>
        <thead>
            <tr>
                <th>Metric</th>
                <?php foreach ($period_labels as $label): ?>
                    <th><?= htmlspecialchars($label) ?></th>
                <?php endforeach; ?>
            </tr>
        </thead>
        <tbody>
            <?php
            $edu_metrics = [
                "total_direct_costs" => "Total Direct Costs",
                "peer_reviewed_direct_costs" => "Peer-Reviewed Costs",
                "k_awards" => "# K Awards",
                "f_awards" => "# F Awards"
            ];
            foreach ($edu_metrics as $field => $label):
            ?>
            <tr>
                <td><?= $label ?></td>
                <?php foreach ($period_labels as $p): ?>
                    <td><?= isset($education[$p][$field]) ? number_format($education[$p][$field], 0) : "" ?></td>
                <?php endforeach; ?>
            </tr>
            <?php endforeach; ?>
        </tbody>
    </table>

    <canvas id="chart_edu"></canvas>
    <script>
        const ctx_edu = document.getElementById("chart_edu").getContext("2d");
        new Chart(ctx_edu, {
            type: 'line',
            data: {
                labels: <?= json_encode($period_labels) ?>,
                datasets: [{
                    label: "Education: Total Direct Costs",
                    data: <?= json_encode(array_map(function($p) use ($education) {
                        return isset($education[$p]) ? floatval($education[$p]['total_direct_costs']) : null;
                    }, $period_labels)) ?>,
                    borderColor: "green",
                    fill: false
                }]
            }
        });
    </script>
</body>
</html>
