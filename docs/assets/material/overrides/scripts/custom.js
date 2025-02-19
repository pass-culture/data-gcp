document.addEventListener("DOMContentLoaded", function () {
    const features = document.querySelectorAll(".feature");
    features.forEach((feature, index) => {
        feature.style.opacity = 0;
        setTimeout(() => {
            feature.style.transition = "opacity 0.5s";
            feature.style.opacity = 1;
        }, index * 200);
    });
});
